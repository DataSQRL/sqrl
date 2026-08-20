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

import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.deployment.model.JdbcStatementModel.PartitionType;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

class PostgresStatementFactoryTest {

  private static EngineConfig config(int partitionTtlDivisor) {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getPropertyAs(
            PostgresStatementFactory.PARTITION_TTL_DIVISOR_KEY, Integer.class))
        .thenReturn(partitionTtlDivisor);
    return engineConfig;
  }

  @Test
  void givenNonRangeOrNoTtl_whenDeriveInterval_thenEmpty() {
    var factory = new PostgresStatementFactory(config(100));

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.HASH, Duration.ofDays(14), ChronoUnit.DAYS))
        .isEmpty();
    assertThat(factory.derivePartitionInterval(PartitionType.RANGE, Duration.ZERO, ChronoUnit.DAYS))
        .isEmpty();
    assertThat(factory.derivePartitionInterval(PartitionType.RANGE, null, null)).isEmpty();
    assertThat(factory.derivePartitionInterval(PartitionType.RANGE, Duration.ofDays(14), null))
        .isEmpty();
  }

  @Test
  void givenRangeAndTtl_whenDeriveInterval_thenWidthReturned() {
    var factory = new PostgresStatementFactory(config(100));

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.RANGE, Duration.ofDays(14), ChronoUnit.DAYS))
        .contains("1 day");
  }

  @Test
  void givenConfiguredDivisor_whenCreate_thenDivisorApplied() {
    var factory = new PostgresStatementFactory(config(4));

    assertThat(
            factory.derivePartitionInterval(
                PartitionType.RANGE, Duration.ofDays(84), ChronoUnit.DAYS))
        .contains("2 weeks");
  }

  @Test
  void givenInvalidDivisor_whenCreate_thenThrows() {
    assertThatThrownBy(() -> new PostgresStatementFactory(config(0)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition-ttl-divisor");
  }
}
