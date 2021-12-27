package ai.dataeng.sqml.planner.optimize2;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;

public class SqrlLogicalCorrelate
    extends Correlate
    implements SqrlLogicalNode {

  public SqrlLogicalCorrelate(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType) {
    super(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
  }

  public SqrlLogicalCorrelate(RelInput input) {
    super(input);
  }

  @Override
  public Correlate copy(RelTraitSet relTraitSet, RelNode relNode, RelNode relNode1,
      CorrelationId correlationId, ImmutableBitSet immutableBitSet, JoinRelType joinRelType) {
    return null;
  }
}
