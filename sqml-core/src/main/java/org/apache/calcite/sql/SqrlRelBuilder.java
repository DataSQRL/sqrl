package org.apache.calcite.sql;

import ai.dataeng.sqml.parser.operator.ImportManager.SourceTableImport;
import ai.dataeng.sqml.planner.nodes.StreamTableScan;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.tools.RelBuilder;

public class SqrlRelBuilder extends RelBuilder {

  public SqrlRelBuilder(Context context,
      RelOptCluster cluster,
      RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
  }

  public SqrlRelBuilder scanStream(String name,SourceTableImport sourceTable) {
    RelOptTable table = relOptSchema.getTableForMember(List.of(name));
    StreamTableScan scan = new StreamTableScan(this.cluster, RelTraitSet.createEmpty(), List.of(), table, sourceTable);

    this.push(scan);
    return this;
  }

  public SqrlRelBuilder scanStream(SourceTableImport ordersImp) {
    RelOptTable table = relOptSchema.getTableForMember(List.of(ordersImp.getTableName().getCanonical()));
    StreamTableScan scan = new StreamTableScan(this.cluster, RelTraitSet.createEmpty(), List.of(), table, ordersImp);

    this.push(scan);
    return this;
  }
}
