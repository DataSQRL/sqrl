package ai.dataeng.sqml.physical;

import lombok.Getter;

@Getter
public class PhysicalPlan {
//  public PhysicalPlan(LogicalPlan logicalPlan) {
//    this.logicalPlan = logicalPlan;
//  }
//
//  LogicalPlan logicalPlan;
//  Map<RelationDefinition, PhysicalPlanNode> mapper = new HashMap<>();
//
//  public <R, C> R accept(PhysicalPlanVisitor<R, C> visitor, C context) {
//    return visitor.visit(this, context);
//  }
//
//  public List<PhysicalPlanNode> getBaseNodes() {
//    return logicalPlan.getTableDefinitions()
//        .stream().map(e->mapper.get(e))
//        .filter(e->e!=null)
//        .collect(Collectors.toList());
//  }
}
