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
package com.datasqrl.plan.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datasqrl.function.IndexableFunction;
import com.datasqrl.plan.index.IndexSelector.NamedTable;
import com.datasqrl.plan.index.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.plan.table.TableStatistic;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class FilterColumnTrimmerTest {

  private static final int EMAIL = 0;
  private static final int GENDER = 1;
  private static final int STATUS = 2;
  private static final int MIN_DISTINCT_COUNT = 20;

  private final FilterColumnTrimmer filterColumnTrimmer =
      new FilterColumnTrimmer(MIN_DISTINCT_COUNT);

  @Test
  void givenEqualityColumnWithFewDistinctValues_whenTrim_thenRemovesTheColumn() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0));
    var emailAndGender = equalityOn(customers, EMAIL, GENDER);

    assertThat(filterColumnTrimmer.trim(emailAndGender))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  @Test
  void givenEqualityColumnWithMinDistinctCount_whenTrim_thenKeepsTheColumn() {
    var customers = customersWith(Map.of(Set.of(STATUS), (double) MIN_DISTINCT_COUNT));
    var emailAndStatus = equalityOn(customers, EMAIL, STATUS);

    assertThat(filterColumnTrimmer.trim(emailAndStatus)).containsSame(emailAndStatus);
  }

  @Test
  void givenNoDistinctCounts_whenTrim_thenKeepsEveryColumn() {
    var customers = customersWith(Map.of());
    var emailAndGender = equalityOn(customers, EMAIL, GENDER);

    assertThat(filterColumnTrimmer.trim(emailAndGender)).containsSame(emailAndGender);
  }

  @Test
  void givenOnlyEqualityColumnsWithFewDistinctValues_whenTrim_thenNothingIsLeftToIndex() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0, Set.of(STATUS), 6.0));
    var genderAndStatus = equalityOn(customers, GENDER, STATUS);

    assertThat(filterColumnTrimmer.trim(genderAndStatus)).isEmpty();
  }

  @Test
  void givenColumnCombinationWithFewDistinctValues_whenTrim_thenRemovesEachOfItsColumns() {
    var customers = customersWith(Map.of(Set.of(GENDER, STATUS), 12.0));
    var emailAndGender = equalityOn(customers, EMAIL, GENDER);

    assertThat(filterColumnTrimmer.trim(emailAndGender))
        .hasValueSatisfying(
            trimmed -> assertThat(trimmed.getEqualityColumns()).containsExactly(EMAIL));
  }

  /** The inequality column can be the sort of a query with a limit, which an index serves. */
  @Test
  void givenInequalityColumnWithFewDistinctValues_whenTrim_thenKeepsTheColumn() {
    var customers = customersWith(Map.of(Set.of(STATUS), 6.0));
    var sortedByStatus = new QueryIndexSummary(customers, Set.of(), Set.of(STATUS), Set.of(), 1.0);

    assertThat(filterColumnTrimmer.trim(sortedByStatus)).containsSame(sortedByStatus);
  }

  @Test
  void givenTrimmedEqualityColumnBesideOtherFilters_whenTrim_thenKeepsTheOtherFilters() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0));
    var textSearch = new IndexableFunctionCall(List.of(EMAIL), mock(IndexableFunction.class));
    var genderWithOtherFilters =
        new QueryIndexSummary(customers, Set.of(GENDER), Set.of(STATUS), Set.of(textSearch), 3.0);

    assertThat(filterColumnTrimmer.trim(genderWithOtherFilters))
        .hasValueSatisfying(
            trimmed -> {
              assertThat(trimmed.getEqualityColumns()).isEmpty();
              assertThat(trimmed.getInequalityColumns()).containsExactly(STATUS);
              assertThat(trimmed.getFunctionCalls()).containsExactly(textSearch);
              assertThat(trimmed.getCount()).isEqualTo(3.0);
            });
  }

  @Test
  void givenMinDistinctCountOfZero_whenTrim_thenKeepsEveryColumn() {
    var customers = customersWith(Map.of(Set.of(GENDER), 2.0));
    var gender = equalityOn(customers, GENDER);

    assertThat(new FilterColumnTrimmer(0).trim(gender)).containsSame(gender);
  }

  private static QueryIndexSummary equalityOn(NamedTable table, Integer... equalityColumns) {
    return new QueryIndexSummary(table, Set.of(equalityColumns), Set.of(), Set.of(), 1.0);
  }

  private static NamedTable customersWith(Map<Set<Integer>, Double> distinctCounts) {
    var relNode = mock(RelNode.class);
    var analysis =
        TableAnalysis.builder()
            .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "Customers"))
            .collapsedRelnode(relNode)
            .originalRelnode(relNode)
            .tableStatistic(TableStatistic.fromHint(1e6).withDistinctCounts(distinctCounts))
            .build();
    return new NamedTable("Customers", "Customers", analysis, null);
  }
}
