package ai.datasqrl.graphql.execution.table.column;

import ai.datasqrl.graphql.execution.table.H2ColumnVisitor2;

public interface H2Column {
  public String getName();
  public String getPhysicalName();

  public <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context);
}
