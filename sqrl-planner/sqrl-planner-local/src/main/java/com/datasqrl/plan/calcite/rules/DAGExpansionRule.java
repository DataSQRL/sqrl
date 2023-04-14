/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.rules;

import com.datasqrl.plan.calcite.table.ScriptRelationalTable;
import com.datasqrl.plan.calcite.table.SourceRelationalTableImpl;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

/**
 *
 */
public abstract class DAGExpansionRule extends RelOptRule {

  public DAGExpansionRule() {
    super(operand(LogicalTableScan.class, any()));

  }

  public RelBuilder getBuilder(LogicalTableScan table) {
    return relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());
  }


  public static class ReadOnly extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalTableScan scan = call.rel(0);
      VirtualRelationalTable vTable = scan.getTable()
          .unwrap(VirtualRelationalTable.class);
      Preconditions.checkArgument(vTable != null);
      ScriptRelationalTable queryTable = vTable.getRoot().getBase();
      if (queryTable.getAssignedStage().get().isRead()) {
        Preconditions.checkArgument(!CalciteUtil.hasNesting(queryTable.getRowType()));
        call.transformTo(queryTable.getConvertedRelNode());
      }
    }

  }

  public static class WriteOnly extends DAGExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalTableScan table = call.rel(0);
      ScriptRelationalTable queryTable = table.getTable().unwrap(ScriptRelationalTable.class);
      SourceRelationalTableImpl sourceTable = table.getTable()
          .unwrap(SourceRelationalTableImpl.class);
      Preconditions.checkArgument(queryTable != null ^ sourceTable != null);
      if (queryTable != null) {
        RelBuilder relBuilder = getBuilder(table);
        relBuilder.push(queryTable.getConvertedRelNode());
        call.transformTo(relBuilder.build());
      }
      if (sourceTable != null) {
        //Leave as is
      }
    }

  }

}
