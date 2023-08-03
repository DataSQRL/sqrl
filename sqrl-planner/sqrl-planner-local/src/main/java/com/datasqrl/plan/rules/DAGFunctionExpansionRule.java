/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.local.generate.TableFunctionBase;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.SourceRelationalTableImpl;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;

/**
 * Expands table function calls for {@link TableFunctionBase} by substituting the
 * parameters into the query.
 */
public class DAGFunctionExpansionRule extends RelOptRule {

  private final Type engineType;

  public DAGFunctionExpansionRule(Type engineType) {
    super(operand(TableFunctionScan.class, any()));
    Preconditions.checkArgument(engineType.isRead());
    this.engineType = engineType;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableFunctionScan scan = call.rel(0);
    TableFunctionBase tblFct = SqrlRexUtil.getCustomTableFunction(scan)
        .map(TableFunctionBase.class::cast).get();
    List<RexNode> operands = ((RexCall)scan.getCall()).getOperands();

    ExecutionStage stage = tblFct.getAssignedStage().get();
    Preconditions.checkArgument(stage.isRead());
    if (stage.getEngine().getType()==engineType) {
      RelNode fctNode = tblFct.getPlannedRelNode();
      RelNode rewrittenNode = CalciteUtil.replaceParameters(fctNode, operands);
      call.transformTo(rewrittenNode);
    }
  }




}
