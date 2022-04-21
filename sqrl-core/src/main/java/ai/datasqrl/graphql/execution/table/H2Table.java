package ai.datasqrl.graphql.execution.table;

import ai.datasqrl.graphql.execution.table.column.Columns;
import ai.datasqrl.graphql.table.Table;
import lombok.Value;

@Value
public class H2Table implements Table {

  Columns columns;
  String name;
}
