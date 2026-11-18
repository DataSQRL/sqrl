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

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.IndexSelectionConfig;
import com.datasqrl.plan.index.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexSelectorConfigByDialectTest {

  private IndexSelectionConfig indexSelection;

  @BeforeEach
  void setUp() {
    indexSelection = mock(IndexSelectionConfig.class);
    when(indexSelection.getMaxIndexes()).thenReturn(12);
    when(indexSelection.getMaxIndexColumns()).thenReturn(4);
    when(indexSelection.getMaxFilterPatterns()).thenReturn(20);
    when(indexSelection.getMinCostReduction()).thenReturn(0.2);
    when(indexSelection.getSmallTableRowLimit()).thenReturn(0);
  }

  @Test
  void givenIndexSelection_whenPostgres_thenUsesConfiguredLimits() {
    var config = new IndexSelectorConfigByDialect(JdbcDialect.Postgres, indexSelection);

    assertThat(config.maxIndexes()).isEqualTo(12);
    assertThat(config.maxIndexColumns(IndexType.BTREE)).isEqualTo(4);
    assertThat(config.maxIndexColumnSets()).isEqualTo(20);
    assertThat(config.getCostImprovementThreshold()).isEqualTo(0.8);
    assertThat(config.getSmallTableRowLimit()).isZero();
  }

  @Test
  void givenIndexSelection_whenPostgresHashIndex_thenStaysSingleColumn() {
    var config = new IndexSelectorConfigByDialect(JdbcDialect.Postgres, indexSelection);

    assertThat(config.maxIndexColumns(IndexType.HASH)).isEqualTo(1);
  }

  @Test
  void givenIndexSelection_whenIceberg_thenKeepsOnePartitionIndex() {
    var config = new IndexSelectorConfigByDialect(JdbcDialect.Iceberg, indexSelection);

    assertThat(config.maxIndexes()).isEqualTo(1);
    assertThat(config.maxIndexColumnSets()).isEqualTo(Integer.MAX_VALUE);
  }
}
