package ai.dataeng.sqml.physical;

public abstract class PhysicalPlanVisitor<R, C> {
  public R visit(PhysicalPlan plan, C context) {
    return null;
  }

  public R visit(PhysicalPlanNode node, C context) {
    return null;
  }
}