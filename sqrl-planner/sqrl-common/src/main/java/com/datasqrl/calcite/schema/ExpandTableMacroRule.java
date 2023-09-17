package com.datasqrl.calcite.schema;


import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public class ExpandTableMacroRule extends RelRule<ExpandTableMacroRule.Config>
    implements TransformationRule {

  public ExpandTableMacroRule() {
    super(ExpandTableMacroRule.Config.DEFAULT);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    LogicalTableFunctionScan node = relOptRuleCall.rel(0);
    RexCall call = (RexCall) node.getCall();
    if (call.getOperator() instanceof SqlUserDefinedTableFunction &&
        ((SqlUserDefinedTableFunction) call.getOperator()).getFunction() instanceof SqrlTableMacro) {
      SqrlTableMacro function = (SqrlTableMacro)((SqlUserDefinedTableFunction) call.getOperator()).getFunction();

      RelNode relNode = CalciteUtil.applyRexShuttleRecursively(function.getViewTransform().get(),
          new ReplaceArgumentWithOperand(((RexCall) node.getCall()).getOperands()));

      Preconditions.checkState(relNode.getRowType().equalsSansFieldNames(node.getRowType()),
          "Not equal:\n " + relNode.getRowType() + " \n " + node.getRowType());

      relOptRuleCall.transformTo(relNode);
    }
  }

  @AllArgsConstructor
  public static class ReplaceArgumentWithOperand extends RexShuttle {
    List<RexNode> operands;
    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
      return operands.get(dynamicParam.getIndex());
    }
  }

  public interface Config extends RelRule.Config {
    ExpandTableMacroRule.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalTableFunctionScan.class).anyInputs())
        .withDescription("ExpandTableMacroRule")
        .as(ExpandTableMacroRule.Config.class);
  }
}