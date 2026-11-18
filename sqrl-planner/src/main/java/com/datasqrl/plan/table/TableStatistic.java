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
package com.datasqrl.plan.table;

import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TableStatistic {

  public static final double DEFAULT_NESTED_MULTIPLIER = 2.0;
  public static final TableStatistic UNKNOWN =
      new TableStatistic(Double.NaN, true, Double.POSITIVE_INFINITY);

  private static final double DEFAULT_ROW_COUNT = 1e15;

  private final double rowCount;
  private final boolean isEstimated;

  /**
   * The most rows the table can hold by its definition, like the number of rows of a {@code VALUES}
   * list, or infinity when nothing limits it. Unlike the row count, this is never an estimate.
   */
  private final double maxRowCount;

  public static TableStatistic fromEstimate(double rowCount) {
    return fromEstimate(rowCount, null);
  }

  /**
   * An estimated row count, unknown if {@code null}, and the most rows the table can hold,
   * unbounded if {@code null}.
   */
  public static TableStatistic fromEstimate(
      @Nullable Double rowCount, @Nullable Double maxRowCount) {
    return new TableStatistic(rowCount == null ? Double.NaN : rowCount, true, bound(maxRowCount));
  }

  public static TableStatistic fromHint(double rowCount) {
    return fromHint(rowCount, null);
  }

  /** A row count from a hint, and the most rows the table can hold, unbounded if {@code null}. */
  public static TableStatistic fromHint(double rowCount, @Nullable Double maxRowCount) {
    return new TableStatistic(rowCount, false, bound(maxRowCount));
  }

  private static double bound(@Nullable Double maxRowCount) {
    return maxRowCount == null ? Double.POSITIVE_INFINITY : maxRowCount;
  }

  @Override
  public String toString() {
    return "Stats=" + rowCount;
  }

  public boolean isUnknown() {
    return Double.isNaN(rowCount);
  }

  /**
   * Whether the table holds at most the given number of rows, by its definition or by a {@code
   * row_count} hint. An estimated row count does not count, since it can be far off, like for a
   * stream joined with a {@code VALUES} list.
   */
  public boolean hasAtMostRows(double rows) {
    return maxRowCount <= rows || (!isEstimated && !isUnknown() && rowCount <= rows);
  }

  public TableStatistic nested() {
    return nested(DEFAULT_NESTED_MULTIPLIER);
  }

  public TableStatistic nested(double multiplier) {
    if (isUnknown()) {
      return UNKNOWN;
    }
    // Unnesting can multiply the rows without limit
    return new TableStatistic(rowCount * multiplier, isEstimated, Double.POSITIVE_INFINITY);
  }

  public double getRowCount() {
    if (isUnknown()) {
      return DEFAULT_ROW_COUNT;
    } else {
      return rowCount;
    }
  }
}
