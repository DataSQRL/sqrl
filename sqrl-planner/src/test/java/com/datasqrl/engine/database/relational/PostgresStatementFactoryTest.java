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
package com.datasqrl.engine.database.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PostgresStatementFactoryTest {

  @ParameterizedTest
  @CsvSource({
    // TTL unit sets the floor when ttl/divisor is smaller
    "14, DAYS, 100, 1 day",
    "30, DAYS, 100, 1 day",
    "36, HOURS, 100, 1 hour",
    "90, MINUTES, 100, 15 minutes",
    "2, WEEKS, 100, 1 week",
    // divisor drives the width for long TTLs, snapped down to the menu
    "84, DAYS, 4, 2 weeks",
    "28, DAYS, 3, 1 week",
    "30, DAYS, 10, 2 days",
    // target below the smallest menu entry falls back to the smallest
    "30, MINUTES, 100, 15 minutes"
  })
  void givenTtlAndDivisor_whenDeriveInterval_thenMenuWidth(
      long amount, ChronoUnit unit, int divisor, String expected) {
    var ttl = unit == ChronoUnit.WEEKS ? Duration.ofDays(amount * 7) : Duration.of(amount, unit);

    assertThat(PostgresStatementFactory.derivePartitionInterval(ttl, unit, divisor))
        .isEqualTo(expected);
  }

  @Test
  void givenNonRangeOrNoTtl_whenDeriveInterval_thenNull() {
    var factory = new PostgresStatementFactory();

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.HASH, Duration.ofDays(14), ChronoUnit.DAYS))
        .isNull();
    assertThat(factory.derivePartitionInterval(PartitionType.RANGE, Duration.ZERO, ChronoUnit.DAYS))
        .isNull();
    assertThat(factory.derivePartitionInterval(PartitionType.RANGE, null, null)).isNull();
    assertThat(factory.derivePartitionInterval(PartitionType.RANGE, Duration.ofDays(14), null))
        .isNull();
  }

  @Test
  void givenRangeAndTtl_whenDeriveInterval_thenWidthReturned() {
    var factory = new PostgresStatementFactory();

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.RANGE, Duration.ofDays(14), ChronoUnit.DAYS))
        .isEqualTo("1 day");
  }

  @Test
  void givenEmptyEngineConfig_whenCreate_thenDefaultDivisorUsed() {
    var factory = new PostgresStatementFactory(new EmptyEngineConfig("postgres"));

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.RANGE, Duration.ofDays(14), ChronoUnit.DAYS))
        .isEqualTo("1 day");
  }

  @Test
  void givenConfiguredDivisor_whenCreate_thenDivisorApplied() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getSetting(
            PostgresStatementFactory.PARTITION_DIVISOR_KEY, Optional.of("100")))
        .thenReturn("4");
    var factory = new PostgresStatementFactory(engineConfig);

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.RANGE, Duration.ofDays(84), ChronoUnit.DAYS))
        .isEqualTo("2 weeks");
  }

  @Test
  void givenInvalidDivisor_whenCreate_thenThrows() {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getSetting(
            PostgresStatementFactory.PARTITION_DIVISOR_KEY, Optional.of("100")))
        .thenReturn("not-a-number");

    assertThatThrownBy(() -> new PostgresStatementFactory(engineConfig))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition-divisor");

    assertThatThrownBy(() -> new PostgresStatementFactory(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition-divisor");
  }
}
