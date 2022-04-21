package ai.datasqrl.physical.database.ddl;

import java.util.List;
import lombok.Value;

@Value
public class CreateTableDDL implements SqlDDLStatement {

  String name;
  List<String> columns;
  List<String> primaryKeys;

  @Override
  public String toSql() {
    String createTable = "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s));";
    String sql = String.format(createTable, name,
        String.join(",", columns),
        String.join(",", primaryKeys));

    return sql;
  }
}
