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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class PostgresPartitionIntervalTest {

  @ParameterizedTest
  @CsvSource({
    // TTL unit sets the floor when ttl/divisor is smaller
    "14, DAYS, 100, 1 day",
    "30, DAYS, 100, 1 day",
    "36, HOURS, 100, 1 hour",
    "90, MINUTES, 100, 15 minutes",
    // divisor drives the width for long TTLs, snapped down to the menu
    "84, DAYS, 4, 2 weeks",
    "28, DAYS, 3, 1 week",
    "30, DAYS, 10, 2 days",
    // target below the smallest menu entry falls back to the smallest
    "30, MINUTES, 100, 15 minutes"
  })
  void givenTtlAndDivisor_whenOf_thenMenuWidth(
      long amount, ChronoUnit unit, int divisor, String expected) {
    var ttl = Duration.of(amount, unit);

    assertThat(PostgresPartitionInterval.of(ttl, unit, divisor).getInterval()).isEqualTo(expected);
  }
}
