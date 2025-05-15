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
package com.datasqrl.calcite.convert;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.immutables.value.Value;

public class SimplePredicateTransform extends RelRule<SimplePredicateTransform.Config>
    implements TransformationRule {

  private final Transform transform;
  private final SqlOperator operator;

  public SimplePredicateTransform(
      SqlOperator operator, Transform transform, RelRule.Config config) {
    super(config);
    this.transform = transform;
    this.operator = operator;
  }

  public static interface Transform {
    RexNode transform(RexBuilder rexBuilder, RexCall predicate);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    var filter = relOptRuleCall.rel(0);

    var rexBuilder = relOptRuleCall.builder().getRexBuilder();
    var hasTransformed = new AtomicBoolean(false);

    var newFilter =
        filter.accept(
            new RexShuttle() {
              @Override
              public RexNode visitCall(RexCall call) {
                /**
                 * Predicate transforms checks if current operator is a boolean and one of the
                 * operands is said function
                 */
                if (isPredicateContainingOp(call)) {
                  hasTransformed.set(true);
                  return transform.transform(rexBuilder, call);
                }

                return super.visitCall(call);
              }

              private boolean isPredicateContainingOp(RexCall call) {
                if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
                  for (RexNode op : call.getOperands()) {
                    if (op instanceof RexCall call1) {
                      if (call1.getOperator().equals(operator)) {
                        return true;
                      }
                    }
                  }
                }
                return false;
              }
            });

    if (hasTransformed.get()) {
      relOptRuleCall.transformTo(newFilter);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface SimplePredicateTransformConfig extends RelRule.Config {

    public static SimplePredicateTransform.Config createConfig(
        SqlOperator sqlOperator, Transform transform) {
      return ImmutableSimplePredicateTransformConfig.builder()
          .operator(sqlOperator)
          .transform(transform)
          .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
          .description("SimplePredicateTransform")
          .operandSupplier(b0 -> b0.operand(LogicalFilter.class).anyInputs())
          .build();
    }

    abstract SqlOperator getOperator();

    abstract Transform getTransform();

    @Override
    default SimplePredicateTransform toRule() {
      return new SimplePredicateTransform(getOperator(), getTransform(), this);
    }
  }
}
