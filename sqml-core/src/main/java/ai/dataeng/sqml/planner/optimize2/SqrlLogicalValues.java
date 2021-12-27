package ai.dataeng.sqml.planner.optimize2;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class SqrlLogicalValues
    extends LogicalValues
    implements SqrlLogicalNode {

  public SqrlLogicalValues(RelOptCluster cluster,
      RelTraitSet traitSet, RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples) {
    super(cluster, traitSet, rowType, tuples);
  }

  public SqrlLogicalValues(RelInput input) {
    super(input);
  }
}
