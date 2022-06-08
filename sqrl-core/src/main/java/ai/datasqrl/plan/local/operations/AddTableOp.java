package ai.datasqrl.plan.local.operations;

import ai.datasqrl.schema.Table;
import lombok.Value;

@Value
public class AddTableOp implements SchemaUpdateOp {
  Table table;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
