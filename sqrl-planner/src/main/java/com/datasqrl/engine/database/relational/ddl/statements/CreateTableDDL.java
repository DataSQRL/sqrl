/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import com.datasqrl.sql.SqlDDLStatement;
import java.util.List;
import java.util.stream.Collectors;
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
    var sql =
        createTable.formatted(
            name,
            columns.stream().map(CreateTableDDL::fieldToSql).collect(Collectors.joining(", ")),
            primaryKeyStr);

    return sql;
  }

  private static String fieldToSql(JdbcStatement.Field field) {
    var sql = new StringBuilder();
    sql.append("\"").append(field.name()).append("\"").append(" ").append(field.type()).append(" ");
    if (!field.nullable()) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }
}
