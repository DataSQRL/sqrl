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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
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

    var equalityColumns =
        removeColumnsThatAddFewDistinctValues(
            uniqueColumns.orElse(queryIndexSummary.getEqualityColumns()),
            tableStatistic.getDistinctCounts());
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
   * Removes each column that multiplies the distinct value combinations of other equality columns
   * by less than {@link #minDistinctCount}. An index on those other columns narrows the rows almost
   * as far without it. The distinct counts of two {@code row_count} hints show this, where the
   * columns of one hint are part of the columns of the other.
   *
   * <p>A column with fewer distinct values than {@link #minDistinctCount} is the case with no other
   * columns: one of its values matches so many rows that an index on it does not pay off. A column
   * has at most as many distinct values as a column combination that it is part of, so a low
   * distinct count of the combination holds for each of its columns.
   *
   * <p>Only an equality column is trimmed for this reason. An inequality column can stand for the
   * sort of a query with a limit, and an index serves that sort however few distinct values the
   * column has.
   */
  private Set<Integer> removeColumnsThatAddFewDistinctValues(
      Set<Integer> equalityColumns, Map<Set<Integer>, Double> distinctCounts) {
    var distinctCountsWithFewestColumnsFirst =
        Stream.concat(
                Stream.of(ColumnsDistinctCount.NO_COLUMNS),
                distinctCounts.entrySet().stream().map(ColumnsDistinctCount::of))
            .sorted(Comparator.comparing(ColumnsDistinctCount::columns, FEWEST_COLUMNS_FIRST))
            .toList();
    var remainingColumns = new LinkedHashSet<>(equalityColumns);
    // A removal holds only while the columns it was compared with stay in the filter
    Set<Integer> columnsThatMustStay = new HashSet<>();

    for (var fewerColumns : distinctCountsWithFewestColumnsFirst) {
      for (var moreColumns : distinctCountsWithFewestColumnsFirst) {
        if (!remainingColumns.containsAll(fewerColumns.columns())
            || !moreColumns.addsColumnsTo(fewerColumns)
            || moreColumns.distinctCount() >= minDistinctCount * fewerColumns.distinctCount()) {
          continue;
        }
        var addedColumns = new HashSet<>(moreColumns.columns());
        addedColumns.removeAll(fewerColumns.columns());
        addedColumns.removeAll(columnsThatMustStay);
        if (remainingColumns.removeAll(addedColumns)) {
          columnsThatMustStay.addAll(fewerColumns.columns());
        }
      }
    }
    return ImmutableSet.copyOf(remainingColumns);
  }

  /** The distinct count of a {@code row_count} hint, with its columns in ascending order. */
  private record ColumnsDistinctCount(List<Integer> columns, double distinctCount) {

    /** No columns have a single value combination: the one without any value. */
    static final ColumnsDistinctCount NO_COLUMNS = new ColumnsDistinctCount(List.of(), 1.0);

    static ColumnsDistinctCount of(Map.Entry<Set<Integer>, Double> distinctCount) {
      return new ColumnsDistinctCount(
          distinctCount.getKey().stream().sorted().toList(), distinctCount.getValue());
    }

    boolean addsColumnsTo(ColumnsDistinctCount fewerColumns) {
      return columns.size() > fewerColumns.columns.size()
          && columns.containsAll(fewerColumns.columns);
    }
  }
}
