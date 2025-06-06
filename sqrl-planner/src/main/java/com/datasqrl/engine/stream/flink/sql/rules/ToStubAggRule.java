/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
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
package com.datasqrl.engine.stream.flink.sql.rules;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.calcite.adapter.enumerable.AggContext;
import org.apache.calcite.adapter.enumerable.AggImplementor;
import org.apache.calcite.adapter.enumerable.AggResetContext;
import org.apache.calcite.adapter.enumerable.AggResultContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableAggFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.immutables.value.Value;

/** Rewrites flink's bridging functions to stub implementable rules for index selection */
public class ToStubAggRule extends RelRule<ToStubAggRule.Config> implements TransformationRule {

  protected ToStubAggRule(Config config) {
    super(config);
  }

  @SneakyThrows
  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalAggregate agg = call.rel(0);

    boolean transformed = false;
    List<AggregateCall> newAggCalls = new ArrayList<>();
    for (AggregateCall aggCall : agg.getAggCallList()) {
      if (aggCall.getAggregation() instanceof BridgingSqlAggFunction) {
        AggregateCall newCall =
            AggregateCall.create(
                wrapInImplementable(aggCall.getAggregation()),
                aggCall.isDistinct(),
                aggCall.isApproximate(),
                aggCall.ignoreNulls(),
                aggCall.getArgList(),
                aggCall.filterArg,
                aggCall.collation,
                aggCall.getType(),
                aggCall.getName());
        newAggCalls.add(newCall);
        transformed = true;
      } else {
        newAggCalls.add(aggCall);
      }
    }

    LogicalAggregate newAgg =
        new LogicalAggregate(
            agg.getCluster(),
            agg.getTraitSet(),
            agg.getInput(),
            agg.indicator,
            agg.getGroupSet(),
            agg.getGroupSets(),
            newAggCalls);

    if (transformed) {
      call.transformTo(newAgg);
    }
  }

  public class StubImplementableAggFnc implements AggregateFunction, ImplementableAggFunction {

    @Override
    public AggImplementor getImplementor(boolean b) {
      return new AggImplementor() {
        @Override
        public List<Type> getStateType(AggContext aggContext) {
          return null;
        }

        @Override
        public void implementReset(AggContext aggContext, AggResetContext aggResetContext) {}

        @Override
        public void implementAdd(AggContext aggContext, AggAddContext aggAddContext) {}

        @Override
        public Expression implementResult(
            AggContext aggContext, AggResultContext aggResultContext) {
          return null;
        }
      };
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory relDataTypeFactory) {
      return null;
    }

    @Override
    public List<FunctionParameter> getParameters() {
      return null;
    }
  }

  private SqlAggFunction wrapInImplementable(SqlAggFunction fn) {
    return new SqlUserDefinedAggFunction(
        fn.getNameAsId(),
        fn.getKind(),
        fn.getReturnTypeInference(),
        fn.getOperandTypeInference(),
        null,
        new StubImplementableAggFnc(),
        fn.requiresOver(),
        fn.requiresOver(),
        fn.requiresGroupOrder());
  }

  @Value.Immutable
  public interface ToStubAggRuleConfig extends RelRule.Config {
    ToStubAggRule.Config DEFAULT =
        ImmutableToStubAggRuleConfig.builder()
            .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .description("ToStubAggRuleConfig")
            .operandSupplier(
                b0 ->
                    b0.operand(LogicalAggregate.class)
                        .oneInput(b1 -> b1.operand(RelNode.class).anyInputs()))
            .build();

    @Override
    default RelOptRule toRule() {
      return new ToStubAggRule(this);
    }
  }
}
