/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rex.RexCall;

import com.datasqrl.calcite.SqrlRexUtil;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.plan.local.generate.QueryTableFunction;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;

/**
 * Expands table function calls for {@link QueryTableFunction} by substituting the
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
    var tblFct = SqrlRexUtil.getCustomTableFunction(scan)
        .filter(fct -> (fct instanceof QueryTableFunction)) //We want to preserve NestedRelationships and other types of table functions
        .map(QueryTableFunction.class::cast).orElse(null);
    if (tblFct==null) {
		return;
	}
    var operands = ((RexCall)scan.getCall()).getOperands();
    var queryTable = tblFct.getQueryTable();
    var stage = queryTable.getAssignedStage().get();
    Preconditions.checkArgument(stage.isRead());
    if (stage.getEngine().getType()==engineType) {
      var fctNode = queryTable.getPlannedRelNode();
      var rewrittenNode = CalciteUtil.replaceParameters(fctNode, operands);
      call.transformTo(rewrittenNode);
    }
  }
}
