package com.datasqrl.physical.database.relational.ddl;

import lombok.Value;

import java.util.List;

@Value
public class CreateTableDDL implements SqlDDLStatement {

  String name;
  List<String> columns;
  List<String> primaryKeys;

  @Override
  public String toSql() {
    String primaryKeyStr = "";
    if (!primaryKeys.isEmpty()) {
      primaryKeyStr = String.format(", PRIMARY KEY (%s)", String.join(",", primaryKeys));
    }
    String createTable = "CREATE TABLE IF NOT EXISTS %s (%s %s);";
    String sql = String.format(createTable, name,
        String.join(",", columns), primaryKeyStr);

    return sql;
  }
}
