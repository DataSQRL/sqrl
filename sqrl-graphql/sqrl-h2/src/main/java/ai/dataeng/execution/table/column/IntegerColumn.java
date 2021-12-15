package ai.dataeng.execution.table.column;

import ai.dataeng.execution.table.H2ColumnVisitor2;
import java.util.Set;
import lombok.Value;

@Value
public class IntegerColumn implements H2Column {
  String name;
  String physicalName;

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context) {
    return visitor.visitIntegerColumn(this, context);
  }
}
