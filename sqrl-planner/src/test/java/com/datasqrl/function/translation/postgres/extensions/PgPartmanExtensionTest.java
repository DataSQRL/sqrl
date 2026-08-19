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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.deployment.model.JdbcStatementModel.PartitionType;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PgPartmanExtensionTest {

  private final PgPartmanExtension extension = new PgPartmanExtension();
  private final EngineConfig engineConfig = new PackageJson.EmptyEngineConfig("postgres");

  private static EngineConfig premakeConfig(String premake) {
    var config = mock(EngineConfig.class);
    when(config.getSetting(PgPartmanExtension.PARTITION_PREMAKE_KEY, Optional.of("4")))
        .thenReturn(premake);
    return config;
  }

  private static CreateTableJdbcStatement table(
      String name, PartitionType partitionType, List<String> partitionKey, Duration ttl) {
    return table(name, partitionType, partitionKey, ttl, "1 day");
  }

  private static CreateTableJdbcStatement table(
      String name,
      PartitionType partitionType,
      List<String> partitionKey,
      Duration ttl,
      String partitionInterval) {
    return new CreateTableJdbcStatement(
        name,
        null,
        List.of(),
        List.of("id", "time"),
        partitionKey,
        partitionType,
        1,
        ttl,
        partitionInterval);
  }

  @Test
  void givenNoPartmanCandidate_whenGetDdl_thenNull() {
    assertThat(
            extension.getDdl(
                List.of(
                    table("plain", PartitionType.NONE, List.of(), Duration.ofDays(30)),
                    table("hashed", PartitionType.HASH, List.of("id"), Duration.ofDays(30)),
                    table("noTtl", PartitionType.RANGE, List.of("time"), Duration.ZERO),
                    table("nullTtl", PartitionType.RANGE, List.of("time"), null),
                    table("noPartitionKey", PartitionType.RANGE, List.of(), Duration.ofDays(30))),
                engineConfig))
        .isNull();
  }

  @Test
  void givenRangeTtlTable_whenGetDdl_thenFullSetupSql() {
    assertThat(
            extension.getDdl(
                List.of(
                    table("Orders_1", PartitionType.RANGE, List.of("time"), Duration.ofDays(30))),
                engineConfig))
        .contains("CREATE SCHEMA IF NOT EXISTS partman")
        .contains("CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman")
        .contains("SELECT partman.create_parent")
        .contains("p_parent_table => 'public.Orders_1'")
        .contains("p_control => 'time'")
        .contains("p_interval => '1 day'")
        .contains("p_premake => 4")
        .contains("p_start_partition => (now() - interval '30 days')::text")
        .contains("UPDATE partman.part_config")
        .contains("retention = '30 days'")
        .contains("retention_keep_table = false")
        .contains("WHERE parent_table = 'public.Orders_1'")
        .doesNotContain("p_type");
  }

  @Test
  void givenMixedTables_whenGetDdl_thenOnlyRangeTtlTablesSortedByName() {
    var sql =
        extension.getDdl(
            List.of(
                table("zebra", PartitionType.RANGE, List.of("ts"), Duration.ofDays(100)),
                table("hashed", PartitionType.HASH, List.of("id"), Duration.ofDays(30)),
                table("alpha", PartitionType.RANGE, List.of("time"), Duration.ofHours(12))),
            engineConfig);

    assertThat(sql).contains("CREATE SCHEMA IF NOT EXISTS partman");
    assertThat(sql).containsOnlyOnce("CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman");
    assertThat(sql).doesNotContain("hashed");
    assertThat(sql.indexOf("alpha")).isLessThan(sql.indexOf("zebra"));
    assertThat(sql).doesNotContain("p_type");
  }

  @Test
  void givenPartitionInterval_whenGetDdl_thenUsedAsIs() {
    assertThat(
            extension.getDdl(
                List.of(
                    table(
                        "Metrics",
                        PartitionType.RANGE,
                        List.of("time"),
                        Duration.ofDays(14),
                        "2 weeks")),
                engineConfig))
        .contains("p_interval => '2 weeks'")
        .contains("retention = '14 days'");
  }

  @Test
  void givenConfiguredPremake_whenGetDdl_thenPremakeApplied() {
    assertThat(
            extension.getDdl(
                List.of(table("Orders", PartitionType.RANGE, List.of("time"), Duration.ofDays(30))),
                premakeConfig("1")))
        .contains("p_premake => 1");
  }

  @Test
  void givenInvalidPremake_whenGetDdl_thenThrows() {
    var tables =
        List.of(table("Orders", PartitionType.RANGE, List.of("time"), Duration.ofDays(30)));

    assertThatThrownBy(() -> extension.getDdl(tables, premakeConfig("0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition-premake");
    assertThatThrownBy(() -> extension.getDdl(tables, premakeConfig("abc")))
        .isInstanceOf(NumberFormatException.class);
  }

  @Test
  void givenTtl_whenFormatRetention_thenLargestWholeUnit() {
    assertThat(PgPartmanExtension.formatRetention(Duration.ofDays(30))).isEqualTo("30 days");
    assertThat(PgPartmanExtension.formatRetention(Duration.ofHours(36))).isEqualTo("1 days");
    assertThat(PgPartmanExtension.formatRetention(Duration.ofHours(12))).isEqualTo("12 hours");
    assertThat(PgPartmanExtension.formatRetention(Duration.ofMinutes(45))).isEqualTo("45 minutes");
  }
}
