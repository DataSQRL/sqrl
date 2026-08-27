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

import static com.datasqrl.engine.database.relational.IndexSelectorConfigByDialect.DEFAULT_COST_THRESHOLD;
import static com.datasqrl.plan.global.IndexSelector.getFallbackIndexColumns;
import static com.datasqrl.plan.global.IndexSelector.servesQueriesWorthIndexing;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.plan.global.IndexSelector.NamedTable;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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

  private static final double FULL_SCAN = 100.0;
  private static final double INDEX_LOOKUP = 5.0;

  /**
   * The number of unrelated queries on the same table must not influence whether an index is
   * created for the query it serves. See <a
   * href="https://github.com/DataSQRL/sqrl/issues/2317">#2317</a>.
   */
  @Test
  void givenIndexServingOneQuery_whenTableHasManyUnrelatedQueries_thenWorthIndexing() {
    for (var numQueries = 1; numQueries <= 100; numQueries++) {
      var before = fullScans(numQueries);
      var after = new HashMap<>(before);
      after.put(before.keySet().iterator().next(), INDEX_LOOKUP);

      assertThat(servesQueriesWorthIndexing(before, after, DEFAULT_COST_THRESHOLD))
          .as("%d queries on the table", numQueries)
          .isTrue();
    }
  }

  @Test
  void givenIndexServingEveryQuery_whenEvaluated_thenWorthIndexing() {
    var before = fullScans(5);
    Map<QueryIndexSummary, Double> after = new LinkedHashMap<>();
    before.keySet().forEach(query -> after.put(query, INDEX_LOOKUP));

    assertThat(servesQueriesWorthIndexing(before, after, DEFAULT_COST_THRESHOLD)).isTrue();
  }

  @Test
  void givenIndexThatBarelyImprovesTheQueryItServes_whenEvaluated_thenNotWorthIndexing() {
    var before = fullScans(5);
    var after = new HashMap<>(before);
    after.put(before.keySet().iterator().next(), FULL_SCAN * 0.99);

    assertThat(servesQueriesWorthIndexing(before, after, DEFAULT_COST_THRESHOLD)).isFalse();
  }

  @Test
  void givenIndexThatImprovesNoQuery_whenEvaluated_thenNotWorthIndexing() {
    var before = fullScans(5);

    assertThat(servesQueriesWorthIndexing(before, new HashMap<>(before), DEFAULT_COST_THRESHOLD))
        .isFalse();
  }

  /**
   * A table without an explicit key gets a synthetic {@code __pk_hash} primary key appended as the
   * last column, so the leading data column is not part of the primary key and needs an index of
   * its own. See <a href="https://github.com/DataSQRL/sqrl/issues/2317">#2317</a>.
   */
  @Test
  void givenSyntheticHashPrimaryKey_whenFallbackToColumnIndexes_thenFirstColumnIsIndexed() {
    var hashKey = primaryKeyIndex(3, "__pk_hash");

    assertThat(getFallbackIndexColumns(queriesFilteringOn(0, 1, 2), hashKey))
        .containsExactly(0, 1, 2);
  }

  @Test
  void givenPrimaryKeyOnFirstColumn_whenFallbackToColumnIndexes_thenFirstColumnIsNotIndexed() {
    var naturalKey = primaryKeyIndex(0, "orderId");

    assertThat(getFallbackIndexColumns(queriesFilteringOn(0, 1, 2), naturalKey))
        .containsExactly(1, 2);
  }

  @Test
  void givenNoPrimaryKeyIndex_whenFallbackToColumnIndexes_thenEveryColumnIsIndexed() {
    assertThat(getFallbackIndexColumns(queriesFilteringOn(0, 1, 2), Optional.empty()))
        .containsExactly(0, 1, 2);
  }

  private static Optional<IndexDefinition> primaryKeyIndex(int column, String columnName) {
    return Optional.of(
        IndexDefinition.getPrimaryKeyIndex("Orders", List.of(column), List.of(columnName)));
  }

  /** The queries only act as carriers of filter columns here, so they need no table. */
  private static List<QueryIndexSummary> queriesFilteringOn(int... columns) {
    return IntStream.of(columns)
        .mapToObj(column -> new QueryIndexSummary(null, Set.of(column), Set.of(), Set.of(), 1.0))
        .collect(Collectors.toList());
  }

  /** The queries only act as identities here, so they do not need a table to belong to. */
  private static Map<QueryIndexSummary, Double> fullScans(int numQueries) {
    Map<QueryIndexSummary, Double> costs = new LinkedHashMap<>();
    for (var column = 0; column < numQueries; column++) {
      costs.put(new QueryIndexSummary(null, Set.of(column), Set.of(), Set.of(), 1.0), FULL_SCAN);
    }
    return costs;
  }
}
