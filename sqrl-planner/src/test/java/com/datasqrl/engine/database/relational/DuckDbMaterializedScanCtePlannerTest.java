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
import static org.mockito.Mockito.when;

import java.util.ArrayDeque;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.junit.jupiter.api.Test;

class DuckDbMaterializedScanCtePlannerTest {

  @Test
  void givenRepeatedSmallTableScan_whenDeterminingCteMaterialization_thenMaterializes() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);
    var scan = scanWithCardinality(1_000.0);

    assertThat(planner.shouldMaterialize(new ArrayDeque<>(List.of(scan, scan)))).isTrue();
  }

  @Test
  void
      givenScanCountAtLargeTableThreshold_whenDeterminingCteMaterialization_thenDoesNotMaterialize() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);
    var scan = scanWithCardinality(Math.pow(2, 23));

    assertThat(planner.shouldMaterialize(new ArrayDeque<>(List.of(scan, scan, scan)))).isFalse();
  }

  @Test
  void givenScanCountAboveLargeTableThreshold_whenDeterminingCteMaterialization_thenMaterializes() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);
    var scan = scanWithCardinality(Math.pow(2, 23));

    assertThat(planner.shouldMaterialize(new ArrayDeque<>(List.of(scan, scan, scan, scan))))
        .isTrue();
  }

  @Test
  void givenUnknownCardinality_whenDeterminingCteMaterialization_thenDoesNotMaterialize() {
    var planner = new DuckDbMaterializedScanCtePlanner(1);
    var scan = scanWithCardinality(null);

    assertThat(planner.shouldMaterialize(new ArrayDeque<>(List.of(scan, scan, scan, scan))))
        .isFalse();
  }

  private TableScan scanWithCardinality(Double cardinality) {
    var scan = mock(TableScan.class);
    var cluster = mock(RelOptCluster.class);
    var metadataQuery = mock(RelMetadataQuery.class);
    when(scan.getCluster()).thenReturn(cluster);
    when(cluster.getMetadataQuery()).thenReturn(metadataQuery);
    when(metadataQuery.getRowCount(scan)).thenReturn(cardinality);
    return scan;
  }
}
