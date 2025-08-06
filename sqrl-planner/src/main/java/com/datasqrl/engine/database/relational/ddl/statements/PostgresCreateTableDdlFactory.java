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

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PostgresCreateTableDdlFactory extends GenericCreateTableDdlFactory {

  public static final String PARTITION_SUFFIX = "_all";

  private final boolean addDefaultPartition;

  @Override
  public String createTableDdl(CreateTableJdbcStatement statement) {
    var sql = super.createTableDdl(statement);
    if (statement.getPartitionType() != PartitionType.NONE) {
      sql +=
          " PARTITION BY %s (%s)"
              .formatted(statement.getPartitionType(), listToSql(statement.getPartitionKey()));
      if (addDefaultPartition) {
        sql += ";\n\n";
        String partitionDefinition =
            switch (statement.getPartitionType()) {
              case RANGE -> "FROM (MINVALUE) TO (MAXVALUE)";
              case HASH -> "WITH (MODULUS 1, REMAINDER 0)";
              default ->
                  throw new UnsupportedOperationException(
                      "Unsupported partition type: " + statement.getPartitionType());
            };
        sql +=
            "CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES %s"
                .formatted(
                    quoteIdentifier(statement.getName() + PARTITION_SUFFIX),
                    quoteIdentifier(statement.getName()),
                    partitionDefinition);
      }
    }
    return sql;
  }
}
