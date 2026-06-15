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
package com.datasqrl.function.translation.postgres.extensions;

import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifier;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.datasqrl.sql.DatabaseTableExtension;
import com.google.auto.service.AutoService;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;

/** Generates the pg_partman setup SQL for RANGE-partitioned tables with a TTL. */
@AutoService(DatabaseTableExtension.class)
public class PgPartmanExtension implements DatabaseTableExtension {

  @Override
  public String getName() {
    return "partman";
  }

  @Override
  public String getDdl(Collection<CreateTableJdbcStatement> createTables) {
    var partmanTables =
        createTables.stream()
            .filter(createTable -> !isNotPartmanTable(createTable))
            .sorted(Comparator.comparing(CreateTableJdbcStatement::getName))
            .toList();

    if (partmanTables.isEmpty()) {
      return null;
    }

    var sb = new StringBuilder();
    sb.append("CREATE SCHEMA IF NOT EXISTS partman;\n");
    sb.append("CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;\n\n");

    partmanTables.forEach(createTable -> appendTableDdl(sb, createTable));

    return sb.toString().trim();
  }

  private void appendTableDdl(StringBuilder sb, CreateTableJdbcStatement createTable) {

    var parentTable = "public." + quoteIdentifier(createTable.getName());
    var ttl = createTable.getTtl();

    // p_type was removed in pg_partman 5.x; the default (range) is what we need
    sb.append(
        """
        SELECT partman.create_parent(
            p_parent_table => '%s',
            p_control => '%s',
            p_interval => '%s',
            p_premake => 4
        );

        """
            .formatted(parentTable, createTable.getPartitionKey().get(0), deriveInterval(ttl)));

    sb.append(
        """
        UPDATE partman.part_config
           SET retention = '%s',
               retention_keep_table = false
         WHERE parent_table = '%s';

        """
            .formatted(formatRetention(ttl), parentTable));
  }

  private boolean isNotPartmanTable(CreateTableJdbcStatement createTable) {
    return createTable.getPartitionType() != PartitionType.RANGE
        || createTable.getTtl() == null
        || createTable.getTtl().isZero()
        || createTable.getPartitionKey().isEmpty();
  }

  /** Picks a partition interval that keeps the partition count reasonable for the given TTL. */
  static String deriveInterval(Duration ttl) {
    long seconds = ttl.getSeconds();
    if (seconds < 7 * 24 * 3600) {
      return "1 day";
    } else if (seconds < 90L * 24 * 3600) {
      return "1 week";
    } else {
      return "1 month";
    }
  }

  static String formatRetention(Duration ttl) {
    long days = ttl.toDays();
    if (days > 0) {
      return days + " days";
    }
    long hours = ttl.toHours();
    if (hours > 0) {
      return hours + " hours";
    }
    return ttl.toMinutes() + " minutes";
  }
}
