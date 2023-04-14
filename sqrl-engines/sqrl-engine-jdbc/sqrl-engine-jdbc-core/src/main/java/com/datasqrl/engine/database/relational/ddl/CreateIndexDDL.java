/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.plan.global.IndexDefinition;
import lombok.Value;

import java.util.List;

@Value
public class CreateIndexDDL implements SqlDDLStatement {

  String indexName;
  String tableName;
  List<String> columns;
  IndexDefinition.Type type;


  @Override
  public String toSql() {
    String createTable = "CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s);";
    String sql = String.format(createTable, indexName, tableName, type.name().toLowerCase(),
        String.join(",", columns));

    return sql;
  }
}
