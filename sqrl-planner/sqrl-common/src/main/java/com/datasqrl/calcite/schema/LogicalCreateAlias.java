package com.datasqrl.calcite.schema;

import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * An aliased table
 */
@Getter
public class LogicalCreateAlias extends SingleRel implements LogicalOp {

  //table to add
  final RelOptTable toTable;

  public LogicalCreateAlias(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable toTable,
      RelNode input) {
    super(cluster, traitSet, input);
    this.toTable = toTable;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}