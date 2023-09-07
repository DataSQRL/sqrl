package com.datasqrl.calcite.schema;

import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;

@Getter
public class LogicalAssignTimestampOp extends AbstractRelNode implements LogicalOp {

  private final int index;
  private final RelNode input;

  public LogicalAssignTimestampOp(RelOptCluster cluster, RelTraitSet traitSet, RelNode input,
      int index) {
    super(cluster, traitSet);
    this.input = input;
    this.index = index;
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
