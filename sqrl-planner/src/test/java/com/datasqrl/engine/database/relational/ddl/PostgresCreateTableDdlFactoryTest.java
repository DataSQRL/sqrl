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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.engine.database.relational.CreateTableJdbcStatement;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class PostgresCreateTableDdlFactoryTest {

  private static CreateTableJdbcStatement stmt(
      PartitionType partitionType, List<String> partitionKey, Duration ttl) {
    return new CreateTableJdbcStatement(
        "orders",
        null,
        List.of(new Field("id", "BIGINT", false, null)),
        List.of("id"),
        partitionKey,
        partitionType,
        1,
        ttl);
  }

  @Test
  void givenNoPartitioning_whenCreateTableDdl_thenPlainCreateTable() {
    var ddl =
        new PostgresCreateTableDdlFactory(true)
            .createTableDdl(stmt(PartitionType.NONE, List.of(), null));
    assertThat(ddl)
        .isEqualTo(
            "CREATE TABLE IF NOT EXISTS \"orders\" (\"id\" BIGINT NOT NULL, PRIMARY KEY (\"id\"))");
  }

  @Test
  void givenNoDefaultPartition_whenCreateTableDdl_thenPartitionByWithoutAllPartition() {
    var ddl =
        new PostgresCreateTableDdlFactory(false)
            .createTableDdl(stmt(PartitionType.RANGE, List.of("ts"), null));
    assertThat(ddl).endsWith("PARTITION BY RANGE (\"ts\")").doesNotContain("_all");
  }

  @Test
  void givenRangePartitionWithTtl_whenCreateTableDdl_thenNoDefaultPartitionForPartman() {
    var ddl =
        new PostgresCreateTableDdlFactory(true)
            .createTableDdl(stmt(PartitionType.RANGE, List.of("ts"), Duration.ofDays(30)));
    assertThat(ddl).endsWith("PARTITION BY RANGE (\"ts\")").doesNotContain("_all");
  }

  @Test
  void givenRangePartitionWithoutTtl_whenCreateTableDdl_thenDefaultPartitionAdded() {
    var ddl =
        new PostgresCreateTableDdlFactory(true)
            .createTableDdl(stmt(PartitionType.RANGE, List.of("ts"), null));
    assertThat(ddl)
        .contains("PARTITION BY RANGE (\"ts\")")
        .contains(
            "CREATE TABLE IF NOT EXISTS \"orders_all\" PARTITION OF \"orders\" FOR VALUES FROM (MINVALUE) TO (MAXVALUE)");
  }

  @Test
  void givenRangePartitionWithZeroTtl_whenCreateTableDdl_thenDefaultPartitionAdded() {
    var ddl =
        new PostgresCreateTableDdlFactory(true)
            .createTableDdl(stmt(PartitionType.RANGE, List.of("ts"), Duration.ZERO));
    assertThat(ddl).contains("\"orders_all\"");
  }

  @Test
  void givenHashPartition_whenCreateTableDdl_thenModulusDefaultPartitionAdded() {
    var ddl =
        new PostgresCreateTableDdlFactory(true)
            .createTableDdl(stmt(PartitionType.HASH, List.of("id"), Duration.ofDays(30)));
    assertThat(ddl)
        .contains("PARTITION BY HASH (\"id\")")
        .contains(
            "CREATE TABLE IF NOT EXISTS \"orders_all\" PARTITION OF \"orders\" FOR VALUES WITH (MODULUS 1, REMAINDER 0)");
  }

  @Test
  void givenListPartition_whenCreateTableDdl_thenThrowsUnsupported() {
    var factory = new PostgresCreateTableDdlFactory(true);
    var listStmt = stmt(PartitionType.LIST, List.of("id"), null);
    assertThatThrownBy(() -> factory.createTableDdl(listStmt))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("LIST");
  }
}
