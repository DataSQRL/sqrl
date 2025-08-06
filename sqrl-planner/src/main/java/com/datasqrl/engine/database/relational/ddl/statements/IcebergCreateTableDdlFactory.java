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

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.google.common.base.Preconditions;

public class IcebergCreateTableDdlFactory extends GenericCreateTableDdlFactory {

  @Override
  public String createTableDdl(CreateTableJdbcStatement stmt) {
    var sql = super.createTableDdl(stmt);
    if (stmt.getPartitionType() != PartitionType.NONE) {
      Preconditions.checkArgument(
          stmt.getPartitionType() == PartitionType.LIST,
          "Iceberg does not support %s partitions",
          stmt.getPartitionType());
      sql += " PARTITIONED BY (%s)".formatted(listToSql(stmt.getPartitionKey()));
    }
    return sql;
  }
}
