package ai.datasqrl.schema;


import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class LogicalDag {
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
