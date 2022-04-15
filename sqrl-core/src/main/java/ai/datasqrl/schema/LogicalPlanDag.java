package ai.datasqrl.schema;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class LogicalPlanDag {
  ShadowingContainer<Table> schema;

  @Override
  public String toString() {
    return "LogicalPlanDag{" +
        "schema=" + schema +
        '}';
  }
}
