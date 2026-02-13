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

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import java.util.EnumSet;
import java.util.Set;

public class IcebergCreateTableDdlFactory extends GenericCreateTableDdlFactory {

  private static final Set<PartitionType> SUPPORTED_PARTITIONS =
      EnumSet.of(PartitionType.NONE, PartitionType.LIST);

  @Override
  public String createTableDdl(CreateTableJdbcStatement stmt) {
    var ddl = super.createTableDdl(stmt);
    if (stmt.getPartitionType() == PartitionType.NONE) {
      return ddl;
    }

    return "%s PARTITIONED BY (%s)".formatted(ddl, listToSql(stmt.getPartitionKey()));
  }

  @Override
  public void validatePartitionType(PartitionType partitionType) {
    if (!SUPPORTED_PARTITIONS.contains(partitionType)) {
      throw new UnsupportedOperationException(
          "Iceberg does not support %s partitions".formatted(partitionType));
    }
  }
}
