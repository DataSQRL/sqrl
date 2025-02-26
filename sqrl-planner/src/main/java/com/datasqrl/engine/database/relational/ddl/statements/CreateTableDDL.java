/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import lombok.Value;

@Value
public class CreateTableDDL implements SqlDDLStatement {

  String name;
  List<String> columns;
  List<String> primaryKeys;

  @Override
  public String getSql() {
    String primaryKeyStr = "";
    if (!primaryKeys.isEmpty()) {
      primaryKeyStr = String.format(" , PRIMARY KEY (%s)", String.join(",", primaryKeys));
    }
    String createTable = "CREATE TABLE IF NOT EXISTS %s (%s%s);";
    String sql = String.format(createTable, name, String.join(",", columns), primaryKeyStr);

    return sql;
  }
}
