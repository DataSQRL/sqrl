/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import java.util.List;
import java.util.stream.Collectors;

import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import com.datasqrl.sql.SqlDDLStatement;

import lombok.Value;

/**
 * TODO: Convert this to SQL node, similar to {@link org.apache.flink.sql.parser.ddl.SqlCreateTable}
 */
@Value
public class CreateTableDDL implements SqlDDLStatement {

  String name;
  List<Field> columns;
  List<String> primaryKeys;

  @Override
  public String getSql() {
    var primaryKeyStr = "";
    if (!primaryKeys.isEmpty()) {
      primaryKeyStr = " , PRIMARY KEY (%s)".formatted(String.join(",", primaryKeys));
    }
    var createTable = "CREATE TABLE IF NOT EXISTS %s (%s%s)";
    var sql = createTable.formatted(name,
        columns.stream().map(CreateTableDDL::fieldToSql).collect(Collectors.joining(", ")), primaryKeyStr);

    return sql;
  }

  private static String fieldToSql(JdbcStatement.Field field) {
    var sql = new StringBuilder();
    sql.append("\"").append(field.getName()).append("\"").append(" ").append(field.getType()).append(" ");
    if (!field.isNullable()) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

}
