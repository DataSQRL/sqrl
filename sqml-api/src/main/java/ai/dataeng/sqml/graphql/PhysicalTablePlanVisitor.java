package ai.dataeng.sqml.graphql;

public interface PhysicalTablePlanVisitor<R,C> {
  R visitPhysicalPlan(PhysicalTablePlan physicalPlan, C context);
  R visitPlanItem(PlanItem planItem, C context);
}
