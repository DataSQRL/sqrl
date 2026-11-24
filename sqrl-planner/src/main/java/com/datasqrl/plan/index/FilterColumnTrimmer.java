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

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

/**
 * Removes the equality columns of a {@link QueryIndexSummary} that add too little to an index,
 * before {@link IndexSelector} generates the index candidates. It reads the distinct counts of the
 * {@code row_count} hints of the table, so a column without such a hint always stays.
 *
 * <p>An inequality column always stays too: it can stand for the sort of a query with a limit, and
 * an index serves that sort however few distinct values the column has.
 */
@AllArgsConstructor
class FilterColumnTrimmer {

  /** See {@link IndexSelectorConfig#getMinDistinctCount()}. */
  private final int minDistinctCount;

  /**
   * @return the query index summary without the trimmed columns, or empty when it has no filter
   *     left to index
   */
  Optional<QueryIndexSummary> trim(QueryIndexSummary queryIndexSummary) {
    var distinctCounts =
        queryIndexSummary.getTable().getAnalysis().getTableStatistic().getDistinctCounts();
    var columnsWithFewDistinctValues = getColumnsWithFewDistinctValues(distinctCounts);
    var equalityColumns =
        queryIndexSummary.getEqualityColumns().stream()
            .filter(column -> !columnsWithFewDistinctValues.contains(column))
            .collect(ImmutableSet.toImmutableSet());

    if (equalityColumns.size() == queryIndexSummary.getEqualityColumns().size()) {
      return Optional.of(queryIndexSummary);
    }
    if (equalityColumns.isEmpty()
        && queryIndexSummary.getInequalityColumns().isEmpty()
        && queryIndexSummary.getFunctionCalls().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        new QueryIndexSummary(
            queryIndexSummary.getTable(),
            equalityColumns,
            queryIndexSummary.getInequalityColumns(),
            queryIndexSummary.getFunctionCalls(),
            queryIndexSummary.getCount()));
  }

  /**
   * One value of such a column matches so many rows that an index on the column does not pay off. A
   * column has at most as many distinct values as a column combination that it is part of, so a low
   * distinct count of the combination holds for each of its columns.
   */
  private Set<Integer> getColumnsWithFewDistinctValues(Map<Set<Integer>, Double> distinctCounts) {
    return distinctCounts.entrySet().stream()
        .filter(distinctCount -> distinctCount.getValue() < minDistinctCount)
        .flatMap(distinctCount -> distinctCount.getKey().stream())
        .collect(Collectors.toUnmodifiableSet());
  }
}
