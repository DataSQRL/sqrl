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
import static org.mockito.Mockito.mock;

import com.datasqrl.plan.table.TableStatistic;
import java.util.ArrayDeque;
import java.util.Collections;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.Test;

class DuckDbMaterializedScanCtePlannerTest {

  @Test
  void givenRepeatedSmallTableScan_whenDeterminingCteMaterialization_thenMaterializes() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);

    assertThat(planner.shouldMaterialize(TableStatistic.fromEstimate(1_000), repeatedScans(2)))
        .isTrue();
  }

  @Test
  void
      givenScanCountAtLargeTableThreshold_whenDeterminingCteMaterialization_thenDoesNotMaterialize() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);

    assertThat(
            planner.shouldMaterialize(
                TableStatistic.fromEstimate(Math.pow(2, 23)), repeatedScans(3)))
        .isFalse();
  }

  @Test
  void givenScanCountAboveLargeTableThreshold_whenDeterminingCteMaterialization_thenMaterializes() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);

    assertThat(
            planner.shouldMaterialize(
                TableStatistic.fromEstimate(Math.pow(2, 23)), repeatedScans(4)))
        .isTrue();
  }

  @Test
  void givenUnknownCardinality_whenDeterminingCteMaterialization_thenDoesNotMaterialize() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);

    assertThat(planner.shouldMaterialize(TableStatistic.UNKNOWN, repeatedScans(4))).isFalse();
  }

  private ArrayDeque<TableScan> repeatedScans(int count) {
    var scan = mock(TableScan.class);
    return new ArrayDeque<>(Collections.nCopies(count, scan));
  }
}
