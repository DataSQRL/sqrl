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

import com.datasqrl.calcite.dialect.ExtendedTrinoSqlDialect;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import java.util.stream.Collectors;

/** Trino has no PRIMARY KEY syntax; keys remain in the deployment model for Iceberg/Flink. */
public class TrinoCreateTableDdlFactory extends GenericCreateTableDdlFactory {
  public TrinoCreateTableDdlFactory() {
    super(ExtendedTrinoSqlDialect.DEFAULT);
  }

  @Override
  public String createTableDdl(CreateTableJdbcStatement statement) {
    return "CREATE TABLE IF NOT EXISTS %s (%s)"
        .formatted(
            quoteIdentifier(statement.getName()),
            statement.getFields().stream().map(this::fieldToSql).collect(Collectors.joining(", ")));
  }
}
