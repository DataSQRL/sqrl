package com.datasqrl.calcite.convert;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.concurrent.atomic.AtomicBoolean;

public class SimplePredicateTransform extends RelRule<SimplePredicateTransform.Config>
    implements TransformationRule {

  private final Transform transform;
  private final SqlOperator operator;

  public SimplePredicateTransform(SqlOperator operator, Transform transform) {
    super(SimplePredicateTransform.Config.DEFAULT);
    this.transform = transform;
    this.operator = operator;
  }

  public static interface Transform {
    RexNode transform(RexBuilder rexBuilder, RexCall predicate);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    LogicalFilter filter = relOptRuleCall.rel(0);

    RexBuilder rexBuilder = relOptRuleCall.builder().getRexBuilder();
    AtomicBoolean hasTransformed = new AtomicBoolean(false);

    RelNode newFilter = filter.accept(new RexShuttle() {
      @Override
      public RexNode visitCall(RexCall call) {
        /**
         * Predicate transforms checks if current operator is a boolean and one of the operands is said function
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
            if (op instanceof RexCall) {
              RexCall call1 = (RexCall) op;
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

  public interface Config extends RelRule.Config {
    SimplePredicateTransform.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class).anyInputs())
        .withDescription("SimpleFilterTransform")
        .as(SimplePredicateTransform.Config.class);
  }
}