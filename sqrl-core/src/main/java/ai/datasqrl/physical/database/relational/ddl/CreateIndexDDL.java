package ai.datasqrl.physical.database.relational.ddl;

import lombok.Value;

import java.util.List;

@Value
public class CreateIndexDDL implements SqlDDLStatement {

  public enum Type {
    HASH, BTREE;
  }

  String indexName;
  String tableName;
  List<String> columns;
  Type type;


  @Override
  public String toSql() {
    String createTable = "CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s);";
    String sql = String.format(createTable, indexName, tableName, type.name().toLowerCase(),
        String.join(",", columns));

    return sql;
  }
}
