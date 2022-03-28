package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ShadowingContainer;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class LogicalPlanDag {
  ShadowingContainer<Table> schema;

  public void mergeSchema(ShadowingContainer<Table> toMerge) {
    schema.addAll(toMerge);
  }

  @Override
  public String toString() {
    return "LogicalPlanDag{" +
        "schema=" + schema +
        '}';
  }
}
