package org.apache.calcite.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;

public class LogicalStream extends AbstractRelNode {

  private final RelNode input;

  public LogicalStream(RelOptCluster cluster,
      RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet);
    this.input = input;
  }

  public static RelNode create(RelNode input) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalStream(cluster, traitSet, input);
  }

  @Override
  protected RelDataType deriveRowType() {
    return ((AbstractRelNode)input).deriveRowType();
  }
}
