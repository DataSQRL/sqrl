package ai.dataeng.sqml.graphql;

import java.util.List;
import java.util.Optional;
import lombok.Value;

@Value
public class Table3 {
  String tableName;

  Optional<List<ContextKey>> contextKeys;
  List<String> primaryKeys;
  List<Column3> columns;
}
