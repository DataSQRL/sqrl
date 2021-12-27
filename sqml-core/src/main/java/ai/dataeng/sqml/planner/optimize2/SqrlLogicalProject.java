package ai.dataeng.sqml.planner.optimize2;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class SqrlLogicalProject
    extends Project
    implements SqrlLogicalNode{

  public SqrlLogicalProject(RelOptCluster cluster, RelTraitSet traits,
      List<RelHint> hints, RelNode input,
      List<? extends RexNode> projects, RelDataType rowType) {
    super(cluster, traits, hints, input, projects, rowType);
  }

  public SqrlLogicalProject(RelInput input) {
    super(input);
  }

  @Override
  public Project copy(RelTraitSet relTraitSet, RelNode relNode, List<RexNode> projects,
      RelDataType relDataType) {
    return new LogicalProject(getCluster(), traitSet, hints, input, projects, rowType);
  }
}
