package com.datasqrl.calcite.convert;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.tools.RelBuilder;

public class SimpleCallTransform extends RelRule<SimpleCallTransform.Config>
    implements TransformationRule {

  private final Transform transform;
  private final SqlOperator operator;

  public SimpleCallTransform(SqlOperator operator, Transform transform) {
    super(SimpleCallTransform.Config.DEFAULT);
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

  public interface Config extends RelRule.Config {
    SimpleCallTransform.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).anyInputs())
        .withDescription("SimpleCallTransform")
        .as(SimpleCallTransform.Config.class);
  }
}