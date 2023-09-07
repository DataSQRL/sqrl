package com.datasqrl.calcite.schema;


import com.datasqrl.calcite.QueryPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

public class RemoveProjectOverTableFunction extends RelRule<RemoveProjectOverTableFunction.Config>
    implements TransformationRule {

  private final QueryPlanner planner;

  public RemoveProjectOverTableFunction(QueryPlanner planner) {
    super(RemoveProjectOverTableFunction.Config.DEFAULT);
    this.planner = planner;
  }

  public static interface Transform {
    RexNode transform(RexBuilder rexBuilder, RexCall call);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    LogicalProject project = relOptRuleCall.rel(0);
    LogicalTableFunctionScan node = relOptRuleCall.rel(1);
    relOptRuleCall.transformTo(node);
  }

  public interface Config extends RelRule.Config {
    RemoveProjectOverTableFunction.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class)
                .oneInput( b1 -> b1.operand(LogicalTableFunctionScan.class).anyInputs()))
        .withDescription("RemoveProjectOverTableFunction")
        .as(RemoveProjectOverTableFunction.Config.class);
  }
}