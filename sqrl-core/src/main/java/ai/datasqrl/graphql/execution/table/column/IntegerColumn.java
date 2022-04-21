package ai.datasqrl.graphql.execution.table.column;

import ai.datasqrl.graphql.execution.table.H2ColumnVisitor2;
import lombok.Value;

@Value
public class IntegerColumn implements H2Column {

  String name;
  String physicalName;

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context) {
    return visitor.visitIntegerColumn(this, context);
  }
}
