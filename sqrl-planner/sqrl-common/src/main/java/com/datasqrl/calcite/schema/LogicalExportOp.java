package com.datasqrl.calcite.schema;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

@Getter
public class LogicalExportOp extends SingleRel implements LogicalOp {

  final RelOptTable table;
  final List<String> sinkPath;

  public LogicalExportOp(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      RelOptTable table, List<String> sinkPath) {
    super(cluster, traitSet, input);
    this.table = table;
    this.sinkPath = sinkPath;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
