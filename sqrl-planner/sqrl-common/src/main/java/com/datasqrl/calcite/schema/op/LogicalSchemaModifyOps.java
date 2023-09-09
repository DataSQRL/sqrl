package com.datasqrl.calcite.schema.op;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

@Getter
public class LogicalSchemaModifyOps extends AbstractRelNode implements LogicalOp {

  final List<RelNode> inputs;

  public LogicalSchemaModifyOps(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> input) {
    super(cluster, traitSet);
    this.inputs = input;
  }

  @Override
  public List<RelNode> getInputs() {
    return inputs;
  }

  @Override
  public <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}