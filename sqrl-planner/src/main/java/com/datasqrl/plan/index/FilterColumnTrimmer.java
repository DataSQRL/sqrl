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

import com.datasqrl.plan.table.TableStatistic;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/**
 * Removes the filter columns of a {@link QueryIndexSummary} that add too little to an index, before
 * {@link IndexSelector} generates the index candidates. It reads the distinct counts of the {@code
 * row_count} hints of the table, so a column without such a hint always stays.
 */
@AllArgsConstructor
class FilterColumnTrimmer {

  private static final Comparator<List<Integer>> FEWEST_COLUMNS_FIRST =
      Comparator.<List<Integer>>comparingInt(List::size)
          .thenComparing(Comparators.lexicographical(Comparator.<Integer>naturalOrder()));

  /** See {@link IndexSelectorConfig#getMinDistinctCount()}. */
  private final int minDistinctCount;

  /**
   * @return the query index summary without the trimmed columns, or empty when it has no filter
   *     left to index
   */
  Optional<QueryIndexSummary> trim(QueryIndexSummary queryIndexSummary) {
    var tableStatistic = queryIndexSummary.getTable().getAnalysis().getTableStatistic();
    var uniqueColumns = findUniqueColumns(queryIndexSummary.getEqualityColumns(), tableStatistic);
    var columnsWithFewDistinctValues =
        getColumnsWithFewDistinctValues(tableStatistic.getDistinctCounts());

    var equalityColumns =
        uniqueColumns.orElse(queryIndexSummary.getEqualityColumns()).stream()
            .filter(column -> !columnsWithFewDistinctValues.contains(column))
            .collect(ImmutableSet.toImmutableSet());
    // The unique columns match at most one row, which needs neither a range nor a sort
    var inequalityColumns =
        uniqueColumns.isPresent() ? Set.<Integer>of() : queryIndexSummary.getInequalityColumns();

    if (equalityColumns.equals(queryIndexSummary.getEqualityColumns())
        && inequalityColumns.equals(queryIndexSummary.getInequalityColumns())) {
      return Optional.of(queryIndexSummary);
    }
    if (equalityColumns.isEmpty()
        && inequalityColumns.isEmpty()
        && queryIndexSummary.getFunctionCalls().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new QueryIndexSummary(
            queryIndexSummary.getTable(),
            equalityColumns,
            inequalityColumns,
            queryIndexSummary.getFunctionCalls(),
            queryIndexSummary.getCount()));
  }

  /**
   * The fewest equality columns that have as many distinct value combinations as the table has
   * rows, so an equality on them matches at most one row and every other filter column adds nothing
   * to an index. An estimated row count does not count, since it can be far off.
   */
  private static Optional<Set<Integer>> findUniqueColumns(
      Set<Integer> equalityColumns, TableStatistic tableStatistic) {
    if (tableStatistic.isUnknown() || tableStatistic.isEstimated()) {
      return Optional.empty();
    }
    return tableStatistic.getDistinctCounts().entrySet().stream()
        .filter(distinctCount -> distinctCount.getValue() >= tableStatistic.getRowCount())
        .map(Map.Entry::getKey)
        .filter(equalityColumns::containsAll)
        .map(columns -> columns.stream().sorted().toList())
        .min(FEWEST_COLUMNS_FIRST)
        .map(ImmutableSet::copyOf);
  }

  /**
   * One value of such a column matches so many rows that an index on the column does not pay off. A
   * column has at most as many distinct values as a column combination that it is part of, so a low
   * distinct count of the combination holds for each of its columns.
   *
   * <p>Only an equality column is trimmed for this reason. An inequality column can stand for the
   * sort of a query with a limit, and an index serves that sort however few distinct values the
   * column has.
   */
  private Set<Integer> getColumnsWithFewDistinctValues(Map<Set<Integer>, Double> distinctCounts) {
    return distinctCounts.entrySet().stream()
        .filter(distinctCount -> distinctCount.getValue() < minDistinctCount)
        .flatMap(distinctCount -> distinctCount.getKey().stream())
        .collect(Collectors.toUnmodifiableSet());
  }
}
