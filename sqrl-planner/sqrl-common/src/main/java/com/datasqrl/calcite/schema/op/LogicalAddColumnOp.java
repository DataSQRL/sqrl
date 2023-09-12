package com.datasqrl.calcite.schema.op;

import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

@Getter
public class LogicalAddColumnOp extends AbstractRelNode implements LogicalOp {

  private final RelNode input;
  //node to add
  final RexNode column;

  //column name
  final String name;
  private final RelOptTable toTable;

  public LogicalAddColumnOp(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode column, String name,
      RelOptTable toTable) {
    super(cluster, traits);
    this.input = input;
    this.column = column;
    this.name = name;
    this.toTable = toTable;
  }

  @Override
  public RelOptTable getTable() {
    return input.getTable();
  }

  @Override
  protected RelDataType deriveRowType() {
    return input.getRowType();
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
