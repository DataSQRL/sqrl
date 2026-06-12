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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class PostgresPartmanSetupFactoryTest {

  private static CreateTableJdbcStatement table(
      String name, PartitionType partitionType, List<String> partitionKey, Duration ttl) {
    return new CreateTableJdbcStatement(
        name, null, List.of(), List.of("id", "time"), partitionKey, partitionType, 1, ttl);
  }

  @Test
  void givenNoPartmanCandidates_whenCreateSetupSql_thenEmpty() {
    var tables =
        List.of(
            table("plain", PartitionType.NONE, List.of(), Duration.ofDays(30)),
            table("hashed", PartitionType.HASH, List.of("id"), Duration.ofDays(30)),
            table("noTtl", PartitionType.RANGE, List.of("time"), Duration.ZERO),
            table("nullTtl", PartitionType.RANGE, List.of("time"), null));
    assertThat(PostgresPartmanSetupFactory.createSetupSql(tables)).isEmpty();
  }

  @Test
  void givenRangeTtlTable_whenCreateSetupSql_thenFullSetupSql() {
    var tables =
        List.of(table("Orders_1", PartitionType.RANGE, List.of("time"), Duration.ofDays(30)));
    var expectedSql =
        """
        CREATE SCHEMA IF NOT EXISTS partman;
        CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

        SELECT partman.create_parent(
            p_parent_table => 'public."Orders_1"',
            p_control      => 'time',
            p_interval     => '1 week',
            p_premake      => 4
        );
        UPDATE partman.part_config
           SET retention            = '30 days',
               retention_keep_table = false
         WHERE parent_table = 'public."Orders_1"';\
        """;
    assertThat(PostgresPartmanSetupFactory.createSetupSql(tables)).hasValue(expectedSql);
  }

  @Test
  void givenMixedTables_whenCreateSetupSql_thenOnlyRangeTtlTablesSortedByName() {
    var tables =
        List.of(
            table("zebra", PartitionType.RANGE, List.of("ts"), Duration.ofDays(100)),
            table("hashed", PartitionType.HASH, List.of("id"), Duration.ofDays(30)),
            table("alpha", PartitionType.RANGE, List.of("time"), Duration.ofHours(12)));
    var sql = PostgresPartmanSetupFactory.createSetupSql(tables).orElseThrow();
    assertThat(sql).doesNotContain("hashed");
    assertThat(sql.indexOf("alpha")).isLessThan(sql.indexOf("zebra"));
    assertThat(sql).doesNotContain("p_type");
  }

  @Test
  void givenTtl_whenDeriveInterval_thenBucketedByDuration() {
    assertThat(PostgresPartmanSetupFactory.deriveInterval(Duration.ofHours(6))).isEqualTo("1 day");
    assertThat(PostgresPartmanSetupFactory.deriveInterval(Duration.ofDays(6))).isEqualTo("1 day");
    assertThat(PostgresPartmanSetupFactory.deriveInterval(Duration.ofDays(7))).isEqualTo("1 week");
    assertThat(PostgresPartmanSetupFactory.deriveInterval(Duration.ofDays(89))).isEqualTo("1 week");
    assertThat(PostgresPartmanSetupFactory.deriveInterval(Duration.ofDays(90)))
        .isEqualTo("1 month");
    assertThat(PostgresPartmanSetupFactory.deriveInterval(Duration.ofDays(365)))
        .isEqualTo("1 month");
  }

  @Test
  void givenTtl_whenFormatRetention_thenLargestWholeUnit() {
    assertThat(PostgresPartmanSetupFactory.formatRetention(Duration.ofDays(30)))
        .isEqualTo("30 days");
    assertThat(PostgresPartmanSetupFactory.formatRetention(Duration.ofHours(36)))
        .isEqualTo("1 days");
    assertThat(PostgresPartmanSetupFactory.formatRetention(Duration.ofHours(12)))
        .isEqualTo("12 hours");
    assertThat(PostgresPartmanSetupFactory.formatRetention(Duration.ofMinutes(45)))
        .isEqualTo("45 minutes");
  }
}
