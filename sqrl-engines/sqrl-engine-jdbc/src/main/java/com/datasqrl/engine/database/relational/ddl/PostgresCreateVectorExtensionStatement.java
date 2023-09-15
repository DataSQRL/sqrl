package com.datasqrl.engine.database.relational.ddl;

public class PostgresCreateVectorExtensionStatement implements SqlDDLStatement {

  @Override
  public String toSql() {
    return "CREATE EXTENSION vector;";
  }
}
