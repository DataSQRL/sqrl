package com.datasqrl.engine.database.relational.ddl;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DropIndexDDL implements SqlDDLStatement {

  String indexName;
  String tableName;

  @Override
  public String toSql() {
    return "DROP INDEX IF EXISTS " + indexName + ";";
  }
}
