package ai.dataeng.sqml.plan;

import ai.dataeng.sqml.planner.PlanNodeId;
import ai.dataeng.sqml.planner.RelationPlan;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import java.util.List;

public class Dag extends PlanNode {

  private final List<RelationPlan> roots;

  public Dag(PlanNodeId id, List<RelationPlan> roots) {
    super(id);
    this.roots = roots;
  }

  @Override
  public List<PlanNode> getSources() {
    return null;
  }

  @Override
  public List<VariableReferenceExpression> getOutputVariables() {
    return null;
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return null;
  }
}
