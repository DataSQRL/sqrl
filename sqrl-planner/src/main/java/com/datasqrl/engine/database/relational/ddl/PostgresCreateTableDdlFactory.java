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

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import java.util.EnumSet;
import java.util.Set;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PostgresCreateTableDdlFactory extends GenericCreateTableDdlFactory {

  private static final Set<PartitionType> SUPPORTED_PARTITIONS =
      EnumSet.of(PartitionType.NONE, PartitionType.RANGE, PartitionType.HASH);

  private static final String PARTITION_SUFFIX = "_all";

  private final boolean addDefaultPartition;

  @Override
  public String createTableDdl(CreateTableJdbcStatement stmt) {
    var ddl = super.createTableDdl(stmt);
    if (stmt.getPartitionType() == PartitionType.NONE) {
      return ddl;
    }

    var partitionDdl =
        "%s PARTITION BY %s (%s)"
            .formatted(ddl, stmt.getPartitionType(), listToSql(stmt.getPartitionKey()));

    if (!addDefaultPartition) {
      return partitionDdl;
    }

    var allPartitionTableName = quoteIdentifier(stmt.getName() + PARTITION_SUFFIX);
    var tableName = quoteIdentifier(stmt.getName());
    var partitionDefinition =
        switch (stmt.getPartitionType()) {
          case RANGE -> "FROM (MINVALUE) TO (MAXVALUE)";
          case HASH -> "WITH (MODULUS 1, REMAINDER 0)";
          default -> null; // validated already, cannot happen
        };

    return """
        %s;

        CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES %s"""
        .formatted(partitionDdl, allPartitionTableName, tableName, partitionDefinition);
  }

  @Override
  public void validatePartitionType(PartitionType partitionType) {
    if (!SUPPORTED_PARTITIONS.contains(partitionType)) {
      throw new UnsupportedOperationException(
          "Postgres does not support %s partitions".formatted(partitionType));
    }
  }
}
