/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.tools.RelBuilder;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.table.SourceRelationalTableImpl;
import com.google.common.base.Preconditions;

/**
 *
 */
public abstract class DAGTableExpansionRule extends RelOptRule {

  public DAGTableExpansionRule() {
    super(operand(LogicalTableScan.class, any()));

  }

  public RelBuilder getBuilder(LogicalTableScan table) {
    return relBuilderFactory.create(table.getCluster(), table.getTable().getRelOptSchema());
  }



  public static class Read extends DAGTableExpansionRule {

    private final EngineFactory.Type engineType;

    public Read(EngineFactory.Type engineType) {
      this.engineType = engineType;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      var scan = call.rel(0);
      ScriptRelationalTable vTable = scan.getTable()
          .unwrap(ScriptRelationalTable.class);
      Preconditions.checkArgument(vTable != null);
      var queryTable = vTable.getRoot();
      var stage = queryTable.getAssignedStage().get();
      if (stage.isRead() && stage.getEngine().getType()==engineType) {
        call.transformTo(queryTable.getPlannedRelNode());
      }
    }

  }

  public static class Write extends DAGTableExpansionRule {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final LogicalTableScan table = call.rel(0);
      PhysicalRelationalTable queryTable = table.getTable().unwrap(PhysicalRelationalTable.class);
      SourceRelationalTableImpl sourceTable = table.getTable()
          .unwrap(SourceRelationalTableImpl.class);
      Preconditions.checkArgument(queryTable != null ^ sourceTable != null);
      if (queryTable != null) {
        var relBuilder = getBuilder(table);
        relBuilder.push(queryTable.getPlannedRelNode());
        call.transformTo(relBuilder.build());
      }
      if (sourceTable != null) {
        //Leave as is
      }
    }

  }

}
