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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexOver;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.rules.logical.FlinkCalcMergeRule;
import org.apache.flink.table.planner.plan.utils.FlinkRelUtil;
import org.immutables.value.Value;

/**
 * {@link FlinkCalcMergeRule} that renders the digests of the merged and the bottom Calc only when
 * the cheap prerequisites of digest equality hold, instead of on every merge.
 */
public class SqrlCalcMergeRule extends RelRule<SqrlCalcMergeRule.SqrlCalcMergeRuleConfig> {

  public static final SqrlCalcMergeRule INSTANCE = SqrlCalcMergeRuleConfig.DEFAULT.toRule();
  public static final SqrlCalcMergeRule STREAM_PHYSICAL_INSTANCE =
      SqrlCalcMergeRuleConfig.STREAM_PHYSICAL.toRule();

  protected SqrlCalcMergeRule(SqrlCalcMergeRuleConfig config) {
    super(config);
  }

  public static RelOptRule replacing(RelOptRule rule) {
    if (rule == FlinkCalcMergeRule.INSTANCE) {
      return INSTANCE;
    }
    if (rule == FlinkCalcMergeRule.STREAM_PHYSICAL_INSTANCE) {
      return STREAM_PHYSICAL_INSTANCE;
    }
    return rule;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Calc topCalc = call.rel(0);
    Calc bottomCalc = call.rel(1);
    if (RexOver.containsOver(topCalc.getProgram())) {
      return false;
    }
    return FlinkRelUtil.isMergeable(topCalc, bottomCalc);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Calc topCalc = call.rel(0);
    Calc bottomCalc = call.rel(1);

    var newCalc = FlinkRelUtil.merge(topCalc, bottomCalc);
    if (mayShareDigest(newCalc, bottomCalc) && newCalc.getDigest().equals(bottomCalc.getDigest())) {
      call.getPlanner().prune(topCalc);
    }
    call.transformTo(newCalc);
  }

  private static boolean mayShareDigest(Calc newCalc, Calc bottomCalc) {
    return newCalc.getRelTypeName().equals(bottomCalc.getRelTypeName())
        && newCalc.getTraitSet().equals(bottomCalc.getTraitSet())
        && newCalc.getRowType().equals(bottomCalc.getRowType())
        && (newCalc.getProgram().getCondition() == null)
            == (bottomCalc.getProgram().getCondition() == null);
  }

  @Value.Immutable
  public interface SqrlCalcMergeRuleConfig extends RelRule.Config {
    SqrlCalcMergeRuleConfig DEFAULT =
        ImmutableSqrlCalcMergeRuleConfig.builder()
            .description("SqrlCalcMergeRule")
            .operandSupplier(
                b0 -> b0.operand(Calc.class).inputs(b1 -> b1.operand(Calc.class).anyInputs()))
            .build();

    SqrlCalcMergeRuleConfig STREAM_PHYSICAL =
        ImmutableSqrlCalcMergeRuleConfig.builder()
            .description("SqrlCalcMergeRule")
            .operandSupplier(
                b0 ->
                    b0.operand(StreamPhysicalCalc.class)
                        .inputs(b1 -> b1.operand(StreamPhysicalCalc.class).anyInputs()))
            .build();

    @Override
    default SqrlCalcMergeRule toRule() {
      return new SqrlCalcMergeRule(this);
    }
  }
}
