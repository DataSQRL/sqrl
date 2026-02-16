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
package com.datasqrl.engine.database.relational.ddl;

import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifier;
import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifiers;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.CreateTableDdlFactory;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import java.util.List;
import java.util.StringJoiner;

public class GenericCreateTableDdlFactory implements CreateTableDdlFactory {

  private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS %s (%s)";

  @Override
  public String createTableDdl(CreateTableJdbcStatement stmt) {
    validatePartitionType(stmt.getPartitionType());
    var tableElements = new StringJoiner(", ");

    // Add field definitions
    stmt.getFields().stream()
        .map(GenericCreateTableDdlFactory::fieldToSql)
        .forEach(tableElements::add);

    // Add primary key constraint if present
    if (!stmt.getPrimaryKey().isEmpty()) {
      tableElements.add("PRIMARY KEY (%s)".formatted(listToSql(stmt.getPrimaryKey())));
    }

    return CREATE_TABLE_TEMPLATE.formatted(
        quoteIdentifier(stmt.getName()), tableElements.toString());
  }

  protected static String listToSql(List<String> columns) {
    return String.join(",", quoteIdentifiers(columns));
  }

  protected static String fieldToSql(Field field) {
    var notNull = field.nullable() ? "" : "NOT NULL";

    return "\"%s\" %s %s".formatted(field.name(), field.type(), notNull).trim();
  }
}
