package ai.datasqrl.plan.local.operations;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Table;
import lombok.Value;

@Value
public class AddNestedQueryOp implements SchemaUpdateOp {
  Table parentTable;
  Table table;
  Name relationshipName;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
