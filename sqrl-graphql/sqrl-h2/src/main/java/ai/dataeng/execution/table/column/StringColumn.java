package ai.dataeng.execution.table.column;

import ai.dataeng.execution.table.H2ColumnVisitor2;
import lombok.Value;

@Value
public class StringColumn implements H2Column {
  String name;
  String physicalName;

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context) {
    return visitor.visitStringColumn(this, context);
  }
}
