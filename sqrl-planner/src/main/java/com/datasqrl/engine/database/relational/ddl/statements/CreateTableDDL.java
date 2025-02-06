/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import java.util.List;

import com.datasqrl.sql.SqlDDLStatement;

import lombok.Value;

@Value
public class CreateTableDDL implements SqlDDLStatement {

  String name;
  List<String> columns;
  List<String> primaryKeys;

  @Override
  public String getSql() {
    var primaryKeyStr = "";
    if (!primaryKeys.isEmpty()) {
      primaryKeyStr = " , PRIMARY KEY (%s)".formatted(String.join(",", primaryKeys));
    }
    var createTable = "CREATE TABLE IF NOT EXISTS %s (%s%s);";
    var sql = createTable.formatted(name,
        String.join(",", columns), primaryKeyStr);

    return sql;
  }
}
