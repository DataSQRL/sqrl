package ai.datasqrl.graphql.execution.table.column;

import ai.datasqrl.graphql.execution.table.H2ColumnVisitor2;

public interface H2Column {

  String getName();

  String getPhysicalName();

  <R, C> R accept(H2ColumnVisitor2<R, C> visitor, C context);
}
