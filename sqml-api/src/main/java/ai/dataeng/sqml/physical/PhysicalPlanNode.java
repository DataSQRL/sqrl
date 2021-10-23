package ai.dataeng.sqml.physical;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public abstract class PhysicalPlanNode {
  public abstract List<Column> getColumns();

  public <R, C> R accept(PhysicalPlanVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

}
