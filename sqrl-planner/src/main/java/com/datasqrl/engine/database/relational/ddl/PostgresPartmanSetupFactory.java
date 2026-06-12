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
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

/**
 * Generates the pg_partman setup SQL for RANGE-partitioned tables with a TTL. The corresponding
 * CREATE TABLE DDL omits the default partition for those tables (see {@link
 * PostgresCreateTableDdlFactory}) so pg_partman owns the partition lifecycle: it premakes child
 * partitions and drops expired ones based on the retention derived from the TTL.
 */
public class PostgresPartmanSetupFactory {

  private PostgresPartmanSetupFactory() {}

  /**
   * Returns the setup SQL, or empty if no table is RANGE-partitioned with a non-zero TTL (in which
   * case no artifact should be emitted).
   */
  public static Optional<String> createSetupSql(Collection<CreateTableJdbcStatement> tables) {
    var partmanTables =
        tables.stream()
            .filter(t -> t.getPartitionType() == PartitionType.RANGE)
            .filter(t -> t.getTtl() != null && !t.getTtl().isZero())
            .filter(t -> !t.getPartitionKey().isEmpty())
            .sorted(Comparator.comparing(CreateTableJdbcStatement::getName))
            .toList();
    if (partmanTables.isEmpty()) {
      return Optional.empty();
    }

    var sb = new StringBuilder();
    sb.append("CREATE SCHEMA IF NOT EXISTS partman;\n");
    sb.append("CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;\n\n");

    for (var table : partmanTables) {
      var parentTable = "public." + quoteIdentifier(table.getName());
      var ttl = table.getTtl();
      // p_type was removed in pg_partman 5.x; the default (range) is what we need
      sb.append(
          """
          SELECT partman.create_parent(
              p_parent_table => '%s',
              p_control      => '%s',
              p_interval     => '%s',
              p_premake      => 4
          );
          """
              .formatted(parentTable, table.getPartitionKey().get(0), deriveInterval(ttl)));
      sb.append(
          """
          UPDATE partman.part_config
             SET retention            = '%s',
                 retention_keep_table = false
           WHERE parent_table = '%s';
          """
              .formatted(formatRetention(ttl), parentTable));
      sb.append("\n");
    }

    return Optional.of(sb.toString().trim());
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
