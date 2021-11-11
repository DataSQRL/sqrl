package ai.dataeng.sqml.graphql;

import java.util.List;
import lombok.Value;

@Value
public class PhysicalTablePlan {
  private List<PlanItem> plan;

  public <R, C> R accept(PhysicalTablePlanVisitor<R, C> visitor, C context) {
    return visitor.visitPhysicalPlan(this, context);
  }
}
