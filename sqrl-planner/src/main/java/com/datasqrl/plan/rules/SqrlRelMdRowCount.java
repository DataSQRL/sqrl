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
package com.datasqrl.plan.rules;

import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.planner.TableAnalysisLookup;
import com.datasqrl.planner.analyzer.TableAnalysis;
import javax.annotation.Nullable;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.NumberUtil;

public class SqrlRelMdRowCount extends RelMdRowCount implements BuiltInMetadata.RowCount.Handler {

  // Row count estimation multipliers for window aggregations
  private static final double TUMBLE_WINDOW_FACTOR = 2.0;
  private static final double SESSION_WINDOW_FACTOR = 2.0;
  private static final double SLIDING_WINDOW_FACTOR = 4.0;

  // Default aggregation ratio when NDV is unknown
  private static final double DEFAULT_AGGREGATION_RATIO = 0.1;

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new SqrlRelMdRowCount(null));

  @Nullable private final TableAnalysisLookup tableLookup;

  public SqrlRelMdRowCount() {
    this(null);
  }

  public SqrlRelMdRowCount(@Nullable TableAnalysisLookup tableLookup) {
    this.tableLookup = tableLookup;
  }

  public RelMetadataProvider getMetadataProvider() {
    return ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, this);
  }

  public Double getRowCount(TableScan scan, RelMetadataQuery mq) {
    if (tableLookup == null) {
      return super.getRowCount(scan, mq);
    }

    var tableAnalysis = tableLookup.lookupViewFromScan(scan);
    if (tableAnalysis != null
        && tableAnalysis.getTableStatistic() != null
        && !tableAnalysis.getTableStatistic().isUnknown()) {
      return tableAnalysis.getTableStatistic().getRowCount();
    }
    return super.getRowCount(scan, mq);
  }

  @Override
  public Double getRowCount(Join rel, RelMetadataQuery mq) {
    double rowCount = super.getRowCount(rel, mq);
    if (rel instanceof EnumerableNestedLoopJoin) {
      rowCount = rowCount + 2 * mq.getRowCount(rel.getLeft());
      // Undo the factor 10 penalty from EnumerableNestedLoopJoin
      rowCount = rowCount / 100;
    }
    return rowCount;
  }

  /**
   * Estimates row count for Correlate (lateral join). For temporal joins (Correlate with Snapshot),
   * the row count is approximately the left input.
   */
  public Double getRowCount(Correlate rel, RelMetadataQuery mq) {
    Double leftRowCount = mq.getRowCount(rel.getLeft());
    if (leftRowCount == null) {
      return null;
    }

    // Check if this is a temporal join pattern (right side contains Snapshot)
    if (containsSnapshot(rel.getRight())) {
      // Temporal join: row count is approximately the left (stream) side
      // Apply a small selectivity for the join condition
      return leftRowCount * 0.9;
    }

    // For regular correlates, estimate based on right side row count
    Double rightRowCount = mq.getRowCount(rel.getRight());
    if (rightRowCount == null) {
      return leftRowCount;
    }

    // Use a conservative estimate: left * sqrt(right) for correlated subqueries
    return leftRowCount * Math.sqrt(rightRowCount);
  }

  /** Checks if the given RelNode tree contains a Snapshot (used in temporal joins). */
  private boolean containsSnapshot(RelNode rel) {
    if (rel instanceof Snapshot) {
      return true;
    }
    for (RelNode input : rel.getInputs()) {
      if (containsSnapshot(input)) {
        return true;
      }
    }
    // Also check single-input nodes
    if (rel instanceof SingleRel singleRel) {
      return containsSnapshot(singleRel.getInput());
    }
    return false;
  }

  /** Estimates row count for Sort with optional LIMIT and OFFSET. */
  public Double getRowCount(Sort rel, RelMetadataQuery mq) {
    Double inputRowCount = mq.getRowCount(rel.getInput());
    if (inputRowCount == null) {
      return null;
    }

    double rowCount = inputRowCount;

    // Apply offset
    if (rel.offset != null && rel.offset instanceof RexLiteral) {
      long offset = RexLiteral.intValue(rel.offset);
      rowCount = Math.max(0, rowCount - offset);
    }

    // Apply fetch (limit)
    if (rel.fetch != null && rel.fetch instanceof RexLiteral) {
      long fetch = RexLiteral.intValue(rel.fetch);
      rowCount = Math.min(rowCount, fetch);
    }

    return rowCount;
  }

  /**
   * Estimates row count for Aggregate based on grouping columns. For window aggregations (aggregate
   * over TableFunctionScan), applies window-specific multipliers.
   */
  public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
    Double inputRowCount = mq.getRowCount(rel.getInput());
    if (inputRowCount == null) {
      return null;
    }

    // For aggregates without grouping, result is always 1 row (or 0 if input is empty)
    if (rel.getGroupCount() == 0) {
      return inputRowCount > 0 ? 1.0 : 0.0;
    }

    // Check if this is a window aggregation (input is a window table function)
    boolean isWindowAggregation = isWindowTableFunction(rel.getInput());

    // Estimate distinct values for grouping columns
    Double distinctRowCount = null;
    try {
      distinctRowCount = mq.getDistinctRowCount(rel.getInput(), rel.getGroupSet(), null);
    } catch (Exception e) {
      // Ignore - will use fallback
    }

    double rowCount;
    if (distinctRowCount != null && distinctRowCount > 0) {
      // Use distinct values estimate, capped at input row count
      rowCount = Math.min(distinctRowCount, inputRowCount);
    } else {
      // Use default aggregation ratio
      rowCount = inputRowCount * DEFAULT_AGGREGATION_RATIO;
    }

    // Apply window-specific multiplier if this is a window aggregation
    if (isWindowAggregation) {
      // For tumbling windows, the multiplier accounts for multiple windows over time
      // The actual number depends on window size vs data time range
      rowCount = Math.min(rowCount * TUMBLE_WINDOW_FACTOR, inputRowCount);
    }

    // Multiply by number of group sets (for GROUPING SETS, ROLLUP, CUBE)
    int groupSetsCount = rel.getGroupSets().size();
    if (groupSetsCount > 1) {
      rowCount = rowCount * groupSetsCount;
    }

    return Math.max(1.0, rowCount);
  }

  /** Checks if the input is a window table function (TUMBLE, HOP, SESSION, CUMULATE). */
  private boolean isWindowTableFunction(RelNode rel) {
    // Skip through Projects to find the TableFunctionScan
    RelNode current = rel;
    while (current instanceof Project) {
      current = ((Project) current).getInput();
    }
    if (current instanceof TableFunctionScan) {
      // Window functions are table functions with specific names
      return true;
    }
    return false;
  }

  /**
   * Estimates row count for TableFunctionScan. For window functions, the row count is approximately
   * the same as input.
   */
  public Double getRowCount(TableFunctionScan rel, RelMetadataQuery mq) {
    if (rel.getInputs().isEmpty()) {
      // No inputs - use default
      return super.getRowCount(rel, mq);
    }

    // For window table functions, row count is approximately the input row count
    Double inputRowCount = mq.getRowCount(rel.getInput(0));
    if (inputRowCount != null) {
      return inputRowCount;
    }

    return super.getRowCount(rel, mq);
  }

  /** Estimates row count for Union: sum of all inputs. */
  public Double getRowCount(Union rel, RelMetadataQuery mq) {
    double rowCount = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double inputRowCount = mq.getRowCount(input);
      if (inputRowCount == null) {
        return null;
      }
      rowCount += inputRowCount;
    }
    // For UNION DISTINCT (not ALL), apply a reduction factor
    if (!rel.all) {
      rowCount = rowCount * 0.9;
    }
    return rowCount;
  }

  /** Estimates row count for Intersect: minimum of all inputs. */
  public Double getRowCount(Intersect rel, RelMetadataQuery mq) {
    double minRowCount = Double.MAX_VALUE;
    for (RelNode input : rel.getInputs()) {
      Double inputRowCount = mq.getRowCount(input);
      if (inputRowCount == null) {
        return null;
      }
      minRowCount = Math.min(minRowCount, inputRowCount);
    }
    // Intersect results in at most the smaller input
    return minRowCount == Double.MAX_VALUE ? null : minRowCount;
  }

  /** Estimates row count for Minus (EXCEPT): minimum of all inputs. */
  public Double getRowCount(Minus rel, RelMetadataQuery mq) {
    if (rel.getInputs().isEmpty()) {
      return null;
    }
    // Minus can produce at most as many rows as the first input
    Double firstInputRowCount = mq.getRowCount(rel.getInput(0));
    if (firstInputRowCount == null) {
      return null;
    }
    // Conservative estimate: apply reduction based on other inputs
    double factor = 1.0;
    for (int i = 1; i < rel.getInputs().size(); i++) {
      Double inputRowCount = mq.getRowCount(rel.getInput(i));
      if (inputRowCount != null && firstInputRowCount > 0) {
        // Reduce by estimated overlap
        factor *= Math.max(0.5, 1.0 - inputRowCount / firstInputRowCount);
      }
    }
    return firstInputRowCount * factor;
  }

  /**
   * Estimates row count for Snapshot (temporal table). Row count is the same as input since
   * Snapshot just provides a point-in-time view.
   */
  public Double getRowCount(Snapshot rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  @Override
  public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof TableScan scan) {
      return getRowCount(scan, mq);
    }
    if (rel instanceof Join join) {
      return getRowCount(join, mq);
    }
    if (rel instanceof Filter filter) {
      return getRowCount(filter, mq);
    }
    if (rel instanceof Correlate correlate) {
      return getRowCount(correlate, mq);
    }
    if (rel instanceof Sort sort) {
      return getRowCount(sort, mq);
    }
    if (rel instanceof Aggregate aggregate) {
      return getRowCount(aggregate, mq);
    }
    if (rel instanceof TableFunctionScan tableFunctionScan) {
      return getRowCount(tableFunctionScan, mq);
    }
    if (rel instanceof Union union) {
      return getRowCount(union, mq);
    }
    if (rel instanceof Intersect intersect) {
      return getRowCount(intersect, mq);
    }
    if (rel instanceof Minus minus) {
      return getRowCount(minus, mq);
    }
    if (rel instanceof Snapshot snapshot) {
      return getRowCount(snapshot, mq);
    }
    return super.getRowCount(rel, mq);
  }

  @Override
  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    return RelMdUtil.estimateFilteredRows(rel.getInput(), rel.getCondition(), mq);
  }

  public static Double getRowCount(TableAnalysis table, QueryIndexSummary constraints) {
    var equalCols = constraints.getEqualityColumns();
    if (table.getPrimaryKey().isDefined() && table.getPrimaryKey().coveredBy(equalCols)) {
      return 1.0;
    }
    return getRowCount(table) * SqrlRelMdSelectivity.getSelectivity(table, constraints);
  }

  public static Double getRowCount(TableAnalysis table) {
    return table.getTableStatistic().getRowCount();
  }
}
