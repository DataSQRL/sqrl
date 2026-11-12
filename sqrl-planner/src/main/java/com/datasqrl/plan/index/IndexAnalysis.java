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

import com.datasqrl.plan.index.IndexSelector.NamedTable;
import java.util.List;
import java.util.function.Predicate;

/**
 * The index summaries of the filters and sorts of a query, and its filters that cannot use an index
 * because they wrap their column in an expression.
 */
public record IndexAnalysis(
    List<QueryIndexSummary> summaries, List<UnindexableFilter> unindexableFilters) {

  /** This analysis without the summaries and filters of the given tables. */
  public IndexAnalysis withoutTables(Predicate<NamedTable> tables) {
    return new IndexAnalysis(
        summaries.stream().filter(summary -> !tables.test(summary.getTable())).toList(),
        unindexableFilters.stream().filter(filter -> !tables.test(filter.table())).toList());
  }
}
