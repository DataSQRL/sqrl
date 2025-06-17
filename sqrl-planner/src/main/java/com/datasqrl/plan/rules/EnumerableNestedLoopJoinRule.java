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
package com.datasqrl.plan.rules;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Additional join rule for Enumerable to make sure that NestedLoopJoin is scored in the cost model
 * since the EnumerableJoinRule will only offer HashJoin as an option for joins with
 * equi-conditions.
 *
 * <p>Copied from Calcite's EnumerableJoinRule
 */
public class EnumerableNestedLoopJoinRule extends ConverterRule {

  /** Default configuration. */
  public static final ConverterRule.Config DEFAULT_CONFIG =
      Config.INSTANCE
          .withConversion(
              LogicalJoin.class,
              Convention.NONE,
              EnumerableConvention.INSTANCE,
              "EnumerableNestedLoopJoinRule")
          .withRuleFactory(EnumerableNestedLoopJoinRule::new);

  public static final RelOptRule INSTANCE =
      EnumerableNestedLoopJoinRule.DEFAULT_CONFIG.toRule(EnumerableNestedLoopJoinRule.class);

  /** Called from the Config. */
  protected EnumerableNestedLoopJoinRule(Config config) {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel) {
    var join = (LogicalJoin) rel;
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : join.getInputs()) {
      if (!(input.getConvention() instanceof EnumerableConvention)) {
        input = convert(input, input.getTraitSet().replace(EnumerableConvention.INSTANCE));
      }
      newInputs.add(input);
    }
    final var left = newInputs.get(0);
    final var right = newInputs.get(1);

    return EnumerableNestedLoopJoin.create(
        left, right, join.getCondition(), join.getVariablesSet(), join.getJoinType());
  }
}
