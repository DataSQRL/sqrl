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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

public class SimpleCallTransform extends RelRule<SimpleCallTransform.Config>
    implements TransformationRule {

  private final Transform transform;
  private final SqlOperator operator;

  public SimpleCallTransform(
      SqlOperator operator, Transform transform, SimpleCallTransform.Config config) {
    super(config);
    this.transform = transform;
    this.operator = operator;
  }

  public static interface Transform {
    RexNode transform(RelBuilder relBuilder, RexCall call);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    var filter = relOptRuleCall.rel(0);

    var hasTransformed = new AtomicBoolean(false);

    var newCall =
        filter.accept(
            new RexShuttle() {
              @Override
              public RexNode visitCall(RexCall call) {
                if (call.getOperator().equals(operator)) {
                  hasTransformed.set(true);
                  return transform.transform(relOptRuleCall.builder(), call);
                }

                return super.visitCall(call);
              }
            });

    if (hasTransformed.get()) {
      relOptRuleCall.transformTo(newCall);
    }
  }

  @Value.Immutable
  public interface SimpleCallTransformConfig extends RelRule.Config {
    public static SimpleCallTransform.Config createConfig(
        SqlOperator sqlOperator, Transform transform) {
      return ImmutableSimpleCallTransformConfig.builder()
          .operator(sqlOperator)
          .transform(transform)
          .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
          .operandSupplier(b0 -> b0.operand(LogicalProject.class).anyInputs())
          .description("SimpleCallTransform")
          .build();
    }

    abstract SqlOperator getOperator();

    abstract Transform getTransform();

    @Override
    default SimpleCallTransform toRule() {
      return new SimpleCallTransform(getOperator(), getTransform(), this);
    }
  }
}
