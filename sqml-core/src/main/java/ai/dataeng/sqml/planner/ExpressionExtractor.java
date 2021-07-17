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
package ai.dataeng.sqml.planner;

import static ai.dataeng.sqml.planner.iterative.Lookup.noLookup;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.plan.AggregationNode;
import ai.dataeng.sqml.plan.ApplyNode;
import ai.dataeng.sqml.plan.FilterNode;
import ai.dataeng.sqml.plan.JoinNode;
import ai.dataeng.sqml.plan.OrderingScheme;
import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.plan.ProjectNode;
import ai.dataeng.sqml.plan.ValuesNode;
import ai.dataeng.sqml.planner.iterative.GroupReference;
import ai.dataeng.sqml.planner.iterative.Lookup;
import ai.dataeng.sqml.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import java.util.List;

public class ExpressionExtractor {

  private ExpressionExtractor() {
  }

  public static List<RowExpression> extractExpressions(PlanNode plan) {
    return extractExpressions(plan, noLookup());
  }

  public static List<RowExpression> extractExpressions(PlanNode plan, Lookup lookup) {
    requireNonNull(plan, "plan is null");
    requireNonNull(lookup, "lookup is null");

    ImmutableList.Builder<RowExpression> expressionsBuilder = ImmutableList.builder();
    plan.accept(new Visitor(true, lookup), expressionsBuilder);
    return expressionsBuilder.build();
  }

  private static class Visitor
      extends SimplePlanVisitor<ImmutableList.Builder<RowExpression>> {

    private final boolean recursive;
    private final Lookup lookup;

    Visitor(boolean recursive, Lookup lookup) {
      this.recursive = recursive;
      this.lookup = requireNonNull(lookup, "lookup is null");
    }

    @Override
    public Void visitPlan(PlanNode node, ImmutableList.Builder<RowExpression> context) {
      if (recursive) {
        return super.visitPlan(node, context);
      }
      return null;
    }

    @Override
    public Void visitGroupReference(GroupReference node,
        ImmutableList.Builder<RowExpression> context) {
      return lookup.resolve(node).accept(this, context);
    }

    @Override
    public Void visitAggregation(AggregationNode node,
        ImmutableList.Builder<RowExpression> context) {
      node.getAggregations().values()
          .forEach(aggregation -> {
            aggregation.getArguments().forEach(context::add);
            aggregation.getFilter().ifPresent(context::add);
            aggregation.getOrderBy()
                .map(OrderingScheme::getOrderByVariables)
                .orElse(ImmutableList.of())
                .forEach(context::add);
          });
      return super.visitAggregation(node, context);
    }

    @Override
    public Void visitFilter(FilterNode node, ImmutableList.Builder<RowExpression> context) {
      context.add(node.getPredicate());
      return super.visitFilter(node, context);
    }

    @Override
    public Void visitProject(ProjectNode node, ImmutableList.Builder<RowExpression> context) {
      context.addAll(node.getAssignments().getExpressions().stream().collect(toImmutableList()));
      return super.visitProject(node, context);
    }

    @Override
    public Void visitJoin(JoinNode node, ImmutableList.Builder<RowExpression> context) {
      node.getFilter().ifPresent(context::add);
      return super.visitJoin(node, context);
    }

    @Override
    public Void visitValues(ValuesNode node, ImmutableList.Builder<RowExpression> context) {
      node.getRows().forEach(context::addAll);
      return super.visitValues(node, context);
    }

    @Override
    public Void visitApply(ApplyNode node, ImmutableList.Builder<RowExpression> context) {
      context.addAll(node.getSubqueryAssignments()
          .getExpressions()
          .stream()
          .collect(toImmutableList()));
      return super.visitApply(node, context);
    }
  }
}
