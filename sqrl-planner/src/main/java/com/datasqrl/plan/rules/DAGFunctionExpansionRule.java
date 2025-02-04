/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.calcite.SqrlRexUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Expands table function calls for {@link QueryTableFunction} by substituting the
 * parameters into the query.
 */
public class DAGFunctionExpansionRule extends RelOptRule {

  private final EngineType engineType;

  public DAGFunctionExpansionRule(EngineType engineType) {
    super(operand(TableFunctionScan.class, any()));
    Preconditions.checkArgument(engineType.isRead());
    this.engineType = engineType;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableFunctionScan scan = call.rel(0);
    QueryTableFunction tblFct = SqrlRexUtil.getCustomTableFunction(scan)
        .filter(fct -> (fct instanceof QueryTableFunction)) //We want to preserve NestedRelationships and other types of table functions
        .map(QueryTableFunction.class::cast).orElse(null);
    if (tblFct==null) return;
    List<RexNode> operands = ((RexCall)scan.getCall()).getOperands();
    QueryRelationalTable queryTable = tblFct.getQueryTable();
    ExecutionStage stage = queryTable.getAssignedStage().get();
    Preconditions.checkArgument(stage.isRead());
    if (stage.getEngine().getType()==engineType) {
      RelNode fctNode = queryTable.getPlannedRelNode();
      RelNode rewrittenNode = CalciteUtil.replaceParameters(fctNode, operands);
      call.transformTo(rewrittenNode);
    }
  }
}
