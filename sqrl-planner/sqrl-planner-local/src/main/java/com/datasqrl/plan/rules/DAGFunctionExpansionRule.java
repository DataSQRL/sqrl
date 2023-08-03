/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.SourceRelationalTableImpl;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

/**
 *
 */
public class DAGFunctionExpansionRule extends RelOptRule {

  private final Type engineType;

  public DAGFunctionExpansionRule(Type engineType) {
    super(operand(TableFunctionScan.class, any()));
    this.engineType = engineType;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    TableFunctionScan scan = call.rel(0);
    VirtualRelationalTable vTable = scan.getTable()
        .unwrap(VirtualRelationalTable.class);
    Preconditions.checkArgument(vTable != null);
    ScriptRelationalTable queryTable = vTable.getRoot().getBase();
    ExecutionStage stage = queryTable.getAssignedStage().get();
    if (stage.isRead() && stage.getEngine().getType()==engineType) {
      Preconditions.checkArgument(!CalciteUtil.hasNesting(queryTable.getRowType()));
      call.transformTo(queryTable.getPlannedRelNode());
    }
  }

  }
