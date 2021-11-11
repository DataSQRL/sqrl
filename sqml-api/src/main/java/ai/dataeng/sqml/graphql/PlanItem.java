package ai.dataeng.sqml.graphql;

import ai.dataeng.sqml.schema2.TypedField;
import lombok.Value;

@Value
public class PlanItem {
  TypedField field;
  Table3 table;

  public <R, C> R accept(PhysicalTablePlanVisitor<R, C> visitor, C context) {
    return visitor.visitPlanItem(this, context);
  }

  public boolean isBatching() {
    return false;
  }
}
