package com.datasqrl.calcite.convert;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
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

  public SimpleCallTransform(SqlOperator operator, Transform transform, SimpleCallTransform.Config config) {
    super(config);
    this.transform = transform;
    this.operator = operator;
  }

  public static interface Transform {
    RexNode transform(RelBuilder relBuilder, RexCall call);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    LogicalProject filter = relOptRuleCall.rel(0);

    AtomicBoolean hasTransformed = new AtomicBoolean(false);

    RelNode newCall = filter.accept(new RexShuttle() {
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
    public static SimpleCallTransform.Config createConfig(SqlOperator sqlOperator, Transform transform) {
      return ImmutableSimpleCallTransformConfig.builder()
          .operator(sqlOperator)
          .transform(transform)
          .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
          .operandSupplier(b0 ->
              b0.operand(LogicalProject.class).anyInputs())
          .description("SimpleCallTransform")
          .build();
    }

    abstract SqlOperator getOperator();
    abstract Transform getTransform();

    @Override default SimpleCallTransform toRule() {
      return new SimpleCallTransform(getOperator(), getTransform(), this);
    }
  }
}