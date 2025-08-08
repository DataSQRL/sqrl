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
public class PostgresCreateTableDdlFactory extends PartitionedCreateTableDdlFactory {

  private static final String PARTITION_SUFFIX = "_all";

  private final boolean addDefaultPartition;

  @Override
  public String createTableDdl(CreateTableJdbcStatement statement) {
    var ddl = super.createTableDdl(statement);

    if (statement.getPartitionType() == PartitionType.NONE || !addDefaultPartition) {
      return ddl;
    }

    var allPartitionTableName = quoteIdentifier(statement.getName() + PARTITION_SUFFIX);
    var tableName = quoteIdentifier(statement.getName());
    var partitionDefinition =
        switch (statement.getPartitionType()) {
          case RANGE -> "FROM (MINVALUE) TO (MAXVALUE)";
          case HASH -> "WITH (MODULUS 1, REMAINDER 0)";
          default -> null; // validated already, cannot happen
        };

    return """
        %s

        CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES %s
        """
        .formatted(ddl, allPartitionTableName, tableName, partitionDefinition);
  }

  @Override
  void validatePartitionType(PartitionType partitionType) {
    if (partitionType != PartitionType.RANGE && partitionType != PartitionType.HASH) {
      throw new UnsupportedOperationException(
          "Postgres does not support %s partitions".formatted(partitionType));
    }
  }
}
