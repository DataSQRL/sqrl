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

import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.deployment.model.JdbcStatementModel.PartitionType;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.sql.DatabaseTableExtension;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;

/** Generates the pg_partman setup SQL for RANGE-partitioned tables with a TTL. */
@AutoService(DatabaseTableExtension.class)
public class PgPartmanExtension implements DatabaseTableExtension {

  public static final String PARTITION_PREMAKE_KEY = "partition-premake";
  private static final String DEFAULT_PREMAKE = "4";

  @Override
  public String getName() {
    return "partman";
  }

  @Override
  public String getDdl(
      Collection<CreateTableJdbcStatement> createTables, EngineConfig engineConfig) {
    var premake =
        Integer.parseInt(
            engineConfig.getSetting(PARTITION_PREMAKE_KEY, Optional.of(DEFAULT_PREMAKE)));
    Preconditions.checkArgument(
        premake >= 1, "%s must be a positive number", PARTITION_PREMAKE_KEY);

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

    partmanTables.forEach(createTable -> appendTableDdl(sb, createTable, premake));

    return sb.toString().trim();
  }

  private void appendTableDdl(StringBuilder sb, CreateTableJdbcStatement createTable, int premake) {

    // pg_partman resolves p_parent_table by matching split_part(name, '.', 2) against
    // pg_class.relname, so the name must be schema-qualified but NOT identifier-quoted:
    // 'public."Readings"' never matches relname 'Readings' and create_parent fails.
    var parentTable = "public." + createTable.getName();
    var ttl = createTable.getTtl();
    var interval =
        Preconditions.checkNotNull(
            createTable.getPartitionInterval(),
            "Missing partition interval for partitioned table %s with TTL",
            createTable.getName());

    var retention = formatRetention(ttl);

    // p_type was removed in pg_partman 5.x; the default (range) is what we need.
    // p_start_partition pre-creates the historical partitions covering the retention window so
    // that catch-up replay does not land in the DEFAULT partition.
    sb.append(
        """
        SELECT partman.create_parent(
            p_parent_table => '%s',
            p_control => '%s',
            p_interval => '%s',
            p_premake => %d,
            p_start_partition => (now() - interval '%s')::text
        );

        """
            .formatted(
                parentTable, createTable.getPartitionKey().get(0), interval, premake, retention));

    sb.append(
        """
        UPDATE partman.part_config
           SET retention = '%s',
               retention_keep_table = false
         WHERE parent_table = '%s';

        """
            .formatted(retention, parentTable));
  }

  private boolean isNotPartmanTable(CreateTableJdbcStatement createTable) {
    return createTable.getPartitionType() != PartitionType.RANGE
        || createTable.getTtl() == null
        || createTable.getTtl().isZero()
        || createTable.getPartitionKey().isEmpty();
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
