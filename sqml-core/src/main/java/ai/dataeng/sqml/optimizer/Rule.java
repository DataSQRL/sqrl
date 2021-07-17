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


import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.planner.PlanNodeIdAllocator;
import ai.dataeng.sqml.planner.PlanVariableAllocator;
import ai.dataeng.sqml.planner.iterative.Lookup;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.Optional;

public interface Rule<T>
{
    /**
     * Returns a pattern to which plan nodes this rule applies.
     */
    Pattern<T> getPattern();

    default boolean isEnabled(Session session)
    {
        return true;
    }

    Result apply(T node, Captures captures, Context context);

    interface Context
    {
        Lookup getLookup();

        PlanNodeIdAllocator getIdAllocator();

        PlanVariableAllocator getVariableAllocator();

        Session getSession();

        StatsProvider getStatsProvider();

        CostProvider getCostProvider();

        void checkTimeoutNotExhausted();
    }

    final class Result
    {
        public static Result empty()
        {
            return new Result(Optional.empty());
        }

        public static Result ofPlanNode(PlanNode transformedPlan)
        {
            return new Result(Optional.of(transformedPlan));
        }

        private final Optional<PlanNode> transformedPlan;

        private Result(Optional<PlanNode> transformedPlan)
        {
            this.transformedPlan = requireNonNull(transformedPlan, "transformedPlan is null");
        }

        public Optional<PlanNode> getTransformedPlan()
        {
            return transformedPlan;
        }

        public boolean isEmpty()
        {
            return !transformedPlan.isPresent();
        }
    }
}
