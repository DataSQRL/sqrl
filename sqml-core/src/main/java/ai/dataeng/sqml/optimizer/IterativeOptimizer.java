/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.optimizer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.planner.PlanNodeIdAllocator;
import ai.dataeng.sqml.planner.PlanVariableAllocator;
import ai.dataeng.sqml.planner.TypeProvider;
import ai.dataeng.sqml.planner.iterative.GroupReference;
import ai.dataeng.sqml.planner.iterative.Lookup;
import com.facebook.presto.matching.Match;
import com.facebook.presto.matching.Matcher;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public class IterativeOptimizer
        implements PlanOptimizer
{
//    private final RuleStatsRecorder stats;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final RuleIndex ruleIndex;

    public IterativeOptimizer(StatsCalculator statsCalculator, CostCalculator costCalculator, Set<Rule<?>> newRules)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.ruleIndex = RuleIndex.builder()
                .register(newRules)
                .build();

    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
        Matcher matcher = new PlanNodeMatcher(lookup);

        Context context = new Context(memo, lookup, idAllocator,
            variableAllocator,
            session);
        boolean planChanged = exploreGroup(memo.getRootGroup(), context, matcher);
        if (!planChanged) {
            return plan;
        }
        return memo.extract();
    }

    private boolean exploreGroup(int group, Context context, Matcher matcher)
    {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(group, context, matcher);

        while (exploreChildren(group, context, matcher)) {
            progress = true;

            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(group, context, matcher)) {
                // no additional matches, so bail out
                break;
            }
        }

        return progress;
    }

    private boolean exploreNode(int group, Context context, Matcher matcher)
    {
        PlanNode node = context.memo.getNode(group);

        boolean done = false;
        boolean progress = false;

        while (!done) {
            context.checkTimeoutNotExhausted();

            done = true;
            Iterator<Rule<?>> possiblyMatchingRules = ruleIndex.getCandidates(node).iterator();
            while (possiblyMatchingRules.hasNext()) {
                Rule<?> rule = possiblyMatchingRules.next();

                if (!rule.isEnabled(context.session)) {
                    continue;
                }

                Rule.Result result = transform(node, rule, matcher, context);

                if (result.getTransformedPlan().isPresent()) {
                    node = context.memo.replace(group, result.getTransformedPlan().get(), rule.getClass().getName());

                    done = false;
                    progress = true;
                }
            }
        }

        return progress;
    }

    private <T> Rule.Result transform(PlanNode node, Rule<T> rule, Matcher matcher, Context context)
    {
        Rule.Result result;

        Match<T> match = matcher.match(rule.getPattern(), node);

        if (match.isEmpty()) {
            return Rule.Result.empty();
        }

        long duration;
        try {
            long start = System.nanoTime();
            result = rule.apply(match.value(), match.captures(), ruleContext(context));
            duration = System.nanoTime() - start;
        }
        catch (RuntimeException e) {
//            stats.recordFailure(rule);
            throw e;
        }
//        stats.record(rule, duration, !result.isEmpty());

        return result;
    }

    private boolean exploreChildren(int group, Context context, Matcher matcher)
    {
        boolean progress = false;

        PlanNode expression = context.memo.getNode(group);
        for (PlanNode child : expression.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(((GroupReference) child).getGroupId(), context, matcher)) {
                progress = true;
            }
        }

        return progress;
    }

    private Rule.Context ruleContext(Context context)
    {
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(context.memo), context.lookup, context.session, context.variableAllocator.getTypes());
        CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(context.memo), context.session);

        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return context.lookup;
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return context.idAllocator;
            }

            @Override
            public PlanVariableAllocator getVariableAllocator()
            {
                return context.variableAllocator;
            }

            @Override
            public Session getSession()
            {
                return context.session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return statsProvider;
            }

            @Override
            public CostProvider getCostProvider()
            {
                return costProvider;
            }

            @Override
            public void checkTimeoutNotExhausted()
            {
                context.checkTimeoutNotExhausted();
            }
        };
    }

    private static class Context
    {
        private final Memo memo;
        private final Lookup lookup;
        private final PlanNodeIdAllocator idAllocator;
        private final PlanVariableAllocator variableAllocator;
        private final Session session;

        public Context(
                Memo memo,
                Lookup lookup,
                PlanNodeIdAllocator idAllocator,
                PlanVariableAllocator variableAllocator,
                Session session)
        {

            this.memo = memo;
            this.lookup = lookup;
            this.idAllocator = idAllocator;
            this.variableAllocator = variableAllocator;
            this.session = session;
        }

        public void checkTimeoutNotExhausted()
        {

        }
    }
}
