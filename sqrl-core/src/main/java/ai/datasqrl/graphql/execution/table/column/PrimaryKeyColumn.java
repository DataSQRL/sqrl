package ai.datasqrl.graphql.execution.table.column;

import ai.datasqrl.graphql.execution.table.H2ColumnVisitor2;
import lombok.Value;

@Value
public class PrimaryKeyColumn implements H2Column {
  H2Column column;

  @Override
  public String getName() {
    return column.getName();
  }

  @Override
  public String getPhysicalName() {
    return column.getPhysicalName();
  }

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context) {
    return visitor.visitPrimaryKeyColumn(this, context);
  }
}
