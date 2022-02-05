package ai.dataeng.execution.table;

import ai.dataeng.execution.table.column.Columns;
import java.util.Optional;
import lombok.Value;

@Value
public class H2Table implements Table {
  Columns columns;
  String name;
}
