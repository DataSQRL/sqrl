package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.logical.RelationDefinition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class PhysicalPlan {
  public PhysicalPlan(LogicalPlan logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  LogicalPlan logicalPlan;
  Map<RelationDefinition, PhysicalPlanNode> mapper = new HashMap<>();

  public <R, C> R accept(PhysicalPlanVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public List<PhysicalPlanNode> getBaseNodes() {
    return logicalPlan.getTableDefinitions()
        .values()
        .stream().map(e->mapper.get(e))
        .filter(e->e!=null)
        .collect(Collectors.toList());
  }
}
