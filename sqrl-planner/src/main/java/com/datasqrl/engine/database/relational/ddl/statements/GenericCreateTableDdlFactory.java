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

import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifier;
import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifiers;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.CreateTableDdlFactory;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import java.util.List;
import java.util.stream.Collectors;

public class GenericCreateTableDdlFactory implements CreateTableDdlFactory {

  @Override
  public String createTableDdl(CreateTableJdbcStatement stmt) {
    var primaryKeyStr = "";
    if (!stmt.getPrimaryKey().isEmpty()) {
      primaryKeyStr = " , PRIMARY KEY (%s)".formatted(listToSql(stmt.getPrimaryKey()));
    }
    var createTable = "CREATE TABLE IF NOT EXISTS %s (%s%s)";
    var sql =
        createTable.formatted(
            quoteIdentifier(stmt.getName()),
            stmt.getFields().stream()
                .map(GenericCreateTableDdlFactory::fieldToSql)
                .collect(Collectors.joining(", ")),
            primaryKeyStr);
    return sql;
  }

  public static String listToSql(List<String> columns) {
    return String.join(",", quoteIdentifiers(columns));
  }

  public static String fieldToSql(Field field) {
    var sql = new StringBuilder();
    sql.append("\"").append(field.name()).append("\"").append(" ").append(field.type()).append(" ");
    if (!field.nullable()) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }
}
