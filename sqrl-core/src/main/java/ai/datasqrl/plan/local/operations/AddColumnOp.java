package ai.datasqrl.plan.local.operations;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor
public class AddColumnOp implements SchemaUpdateOp {
  Table table;
  Node node;
  Column column;
  boolean isTimestamp;

  public AddColumnOp(Table table, Node node, Column column) {
    this.table = table;
    this.node = node;
    this.column = column;
    this.isTimestamp = false;
  }

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }

  public AddColumnOp asTimestamp() {
    return new AddColumnOp(table,node,column,true);
  }
}
