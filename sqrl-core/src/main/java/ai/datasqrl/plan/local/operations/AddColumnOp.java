package ai.datasqrl.plan.local.operations;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import lombok.Value;

@Value
public class AddColumnOp implements SchemaUpdateOp {
  Table table;
  Node node;
  Column column;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
