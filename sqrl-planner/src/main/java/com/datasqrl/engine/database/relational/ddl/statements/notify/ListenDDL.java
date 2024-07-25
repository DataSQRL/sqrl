package com.datasqrl.engine.database.relational.ddl.statements.notify;

import com.datasqrl.sql.SqlDDLStatement;
import lombok.AllArgsConstructor;


@AllArgsConstructor
public class ListenDDL implements SqlDDLStatement {

  String tableName;

  @Override
  public String getSql() {
    return "LISTEN " + tableName + "_notify";
  }
}
