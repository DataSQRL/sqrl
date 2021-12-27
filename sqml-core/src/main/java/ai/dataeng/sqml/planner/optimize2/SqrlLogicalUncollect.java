package ai.dataeng.sqml.planner.optimize2;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class SqrlLogicalUncollect
    extends Uncollect
    implements SqrlLogicalNode {

  public SqrlLogicalUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input,
      boolean withOrdinality, List<String> itemAliases) {
    super(cluster, traitSet, input, withOrdinality, itemAliases);
  }

  public SqrlLogicalUncollect(RelInput input) {
    super(input);
  }
}
