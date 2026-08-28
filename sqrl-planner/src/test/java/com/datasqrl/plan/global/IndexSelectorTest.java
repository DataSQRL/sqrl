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
package com.datasqrl.plan.global;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.plan.global.IndexSelector.NamedTable;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class IndexSelectorTest {

  @Test
  void givenProjectedColumnSort_whenSummarize_thenIdentifiesColumn() {
    var fieldType = mock(RelDataType.class);
    var field = mock(RelDataTypeField.class);
    when(field.getType()).thenReturn(fieldType);
    var rowType = mock(RelDataType.class);
    when(rowType.getFieldList()).thenReturn(List.of(field));

    var summary =
        QueryIndexSummary.ofSort(
                new NamedTable("orders", "orders", null, null), RexInputRef.of(0, rowType))
            .orElseThrow();

    assertThat(summary.getInequalityColumns()).containsExactly(0);
  }

  @Test
  void givenSort_whenGenerateIndexCandidates_thenBtreeUsesDefaultSortOrder() {
    var rowType = mock(RelDataType.class);
    when(rowType.getFieldNames()).thenReturn(List.of("col_a"));
    var relNode = mock(RelNode.class);
    when(relNode.getRowType()).thenReturn(rowType);
    var tableAnalysis =
        TableAnalysis.builder()
            .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "orders"))
            .collapsedRelnode(relNode)
            .originalRelnode(relNode)
            .build();
    var table = new NamedTable("orders", "orders", tableAnalysis, null);
    var summary = new QueryIndexSummary(table, Set.of(), Set.of(0), Set.of(), 1.0);
    var config = mock(IndexSelectorConfig.class);
    when(config.supportedIndexTypes()).thenReturn(EnumSet.of(IndexType.BTREE));
    when(config.maxIndexColumns(IndexType.BTREE)).thenReturn(1);

    var candidates = new IndexSelector(null, config, Map.of()).generateIndexCandidates(summary);

    assertThat(candidates)
        .singleElement()
        .satisfies(
            index -> {
              assertThat(index.getColumns()).containsExactly(0);
              assertThat(index.getDirections()).containsExactly(Direction.ASCENDING);
            });
  }
}
