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
package com.datasqrl.plan.table;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TableStatisticTest {

  @Test
  void givenBoundWithinLimit_whenHasAtMostRows_thenTrue() {
    var statistic = TableStatistic.fromEstimate(6.0, 6.0);

    assertThat(statistic.hasAtMostRows(100)).isTrue();
  }

  @Test
  void givenBoundAboveLimit_whenHasAtMostRows_thenFalse() {
    var statistic = TableStatistic.fromEstimate(101.0, 101.0);

    assertThat(statistic.hasAtMostRows(100)).isFalse();
  }

  @Test
  void givenSmallEstimateWithoutBound_whenHasAtMostRows_thenFalse() {
    // A stream joined with a VALUES list can be estimated at a few rows
    var statistic = TableStatistic.fromEstimate(5.0, null);

    assertThat(statistic.hasAtMostRows(100)).isFalse();
  }

  @Test
  void givenSmallRowCountHint_whenHasAtMostRows_thenTrue() {
    var statistic = TableStatistic.fromHint(10, null);

    assertThat(statistic.hasAtMostRows(100)).isTrue();
  }

  @Test
  void givenLargeRowCountHint_whenHasAtMostRows_thenFalse() {
    var statistic = TableStatistic.fromHint(1e6);

    assertThat(statistic.hasAtMostRows(100)).isFalse();
  }

  @Test
  void givenUnknownStatistic_whenHasAtMostRows_thenFalse() {
    assertThat(TableStatistic.UNKNOWN.hasAtMostRows(100)).isFalse();
  }

  @Test
  void givenUnknownRowCountWithBound_whenHasAtMostRows_thenTrue() {
    var statistic = TableStatistic.fromEstimate(null, 1.0);

    assertThat(statistic.hasAtMostRows(100)).isTrue();
  }
}
