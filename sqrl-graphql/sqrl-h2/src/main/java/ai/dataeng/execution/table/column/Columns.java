package ai.dataeng.execution.table.column;

import ai.dataeng.execution.table.H2ColumnVisitor2;
import java.util.List;
import lombok.Value;

@Value
public class Columns {
  //Todo: Verify that all columns have a unique logical name;
  List<H2Column> columns;

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context) {
    return visitor.visitColumns(this, context);
  }
}
