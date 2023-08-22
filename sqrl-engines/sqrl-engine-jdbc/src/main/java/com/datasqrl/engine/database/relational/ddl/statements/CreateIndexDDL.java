/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.function.IndexType;
import java.util.stream.Collectors;
import lombok.Value;

import java.util.List;

@Value
public class CreateIndexDDL implements SqlDDLStatement {

  String indexName;
  String tableName;
  List<String> columns;
  IndexType type;


  @Override
  public String toSql() {
    String indexType, columnExpression;
    switch (type) {
      case TEXT:
        columnExpression = String.format("to_tsvector('english', %s )",
            columns.stream().map(col -> String.format("coalesce(%s, '')", col)).collect(
                Collectors.joining(" || ' ' || ")));
        indexType = "GIN";
        break;
      default:
        columnExpression = String.join(",", columns);
        indexType = type.name().toLowerCase();
    }

    String createTable = "CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s);";
    String sql = String.format(createTable, indexName, tableName, indexType,
        columnExpression);
    return sql;
  }
}
