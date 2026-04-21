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
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

/**
 * Row count estimation handler adapted from Flink's FlinkRelMdRowCount. Provides improved row count
 * estimates for various relational operators.
 */
public class SqrlRelMdRowCount extends RelMdRowCount implements BuiltInMetadata.RowCount.Handler {

  // Row count estimation multipliers for window aggregations (from Flink)
  private static final double TUMBLE_WINDOW_FACTOR = 2.0;
  private static final double SESSION_WINDOW_FACTOR = 2.0;
  private static final double SLIDING_WINDOW_OVERLAP_FACTOR = 4.0;

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

  // ==================== TableScan ====================

  public Double getRowCount(TableScan scan, RelMetadataQuery mq) {
    if (tableLookup == null) {
      return scan.estimateRowCount(mq);
    }

    var tableAnalysis = tableLookup.lookupViewFromScan(scan);
    if (tableAnalysis != null
        && tableAnalysis.getTableStatistic() != null
        && !tableAnalysis.getTableStatistic().isUnknown()) {
      return tableAnalysis.getTableStatistic().getRowCount();
    }
    return scan.estimateRowCount(mq);
  }

  // ==================== Values ====================

  /** Row count for Values is the number of tuples. */
  public Double getRowCount(Values rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }

  // ==================== Project ====================

  /** Project passes through row count unchanged. */
  public Double getRowCount(Project rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  // ==================== Filter ====================

  @Override
  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    return RelMdUtil.estimateFilteredRows(rel.getInput(), rel.getCondition(), mq);
  }

  // ==================== Calc (Filter + Project) ====================

  /** Calc combines Filter and Project - estimate filtered rows based on condition. */
  public Double getRowCount(Calc rel, RelMetadataQuery mq) {
    var program = rel.getProgram();
    if (program.getCondition() != null) {
      var condition = program.expandLocalRef(program.getCondition());
      return RelMdUtil.estimateFilteredRows(rel.getInput(), condition, mq);
    }
    return mq.getRowCount(rel.getInput());
  }

  // ==================== Exchange ====================

  /** Exchange passes through row count - data redistribution doesn't change cardinality. */
  public Double getRowCount(Exchange rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  // ==================== Sort/Limit ====================

  /** Sort with optional LIMIT and OFFSET. Formula: min(fetch, max(0, inputRowCount - offset)) */
  public Double getRowCount(Sort rel, RelMetadataQuery mq) {
    Double inputRowCount = mq.getRowCount(rel.getInput());
    if (inputRowCount == null) {
      return null;
    }

    double rowCount = inputRowCount;

    // Apply offset: max(inputRowCount - offset, 0)
    if (rel.offset != null && rel.offset instanceof RexLiteral) {
      long offset = RexLiteral.intValue(rel.offset);
      rowCount = Math.max(0, rowCount - offset);
    }

    // Apply fetch (limit): min(rowCount, fetch)
    if (rel.fetch != null && rel.fetch instanceof RexLiteral) {
      long fetch = RexLiteral.intValue(rel.fetch);
      rowCount = Math.min(rowCount, fetch);
    }

    return rowCount;
  }

  // ==================== Aggregate ====================

  /**
   * Aggregate row count estimation. - No grouping: returns 1.0 - With grouping: distinctRowCount *
   * groupSetsCount - Fallback: inputRowCount * aggregationRatio
   */
  public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
    Double inputRowCount = mq.getRowCount(rel.getInput());
    if (inputRowCount == null) {
      return null;
    }

    int groupCount = rel.getGroupCount();

    // For aggregates without grouping, result is always 1 row
    if (groupCount == 0) {
      return 1.0;
    }

    // Check if this is a window aggregation
    boolean isWindowAggregation = isWindowTableFunction(rel.getInput());

    // Try to get distinct row count for grouping columns
    Double distinctRowCount = null;
    try {
      distinctRowCount = mq.getDistinctRowCount(rel.getInput(), rel.getGroupSet(), null);
    } catch (Exception e) {
      // Ignore - will use fallback
    }

    double rowCount;
    if (distinctRowCount != null && distinctRowCount > 0) {
      rowCount = Math.min(distinctRowCount, inputRowCount);
    } else {
      // Flink's fallback: aggregationRatio based on group count
      double aggregationRatio = getAggregationRatioIfNdvUnavailable(groupCount);
      rowCount = inputRowCount * aggregationRatio;
    }

    // Apply window-specific multiplier
    if (isWindowAggregation) {
      double windowFactor = getWindowExpansionFactor(rel.getInput());
      rowCount = Math.min(rowCount * windowFactor, inputRowCount);
    }

    // Multiply by number of group sets (GROUPING SETS, ROLLUP, CUBE)
    int groupSetsCount = rel.getGroupSets().size();
    if (groupSetsCount > 1) {
      rowCount = rowCount * groupSetsCount;
    }

    return Math.max(1.0, rowCount);
  }

  /** Flink's aggregation ratio when NDV is unavailable. Decreases as group count increases. */
  private double getAggregationRatioIfNdvUnavailable(int groupCount) {
    if (groupCount == 0) {
      return 1.0;
    }
    // Flink uses a decreasing ratio based on group count
    // More grouping columns = more distinct groups = higher ratio
    return Math.min(1.0, 0.1 * Math.pow(1.5, groupCount - 1));
  }

  /**
   * Get window expansion factor based on window type. - Tumbling: 2.0 - Session: 2.0 - Sliding with
   * overlap: 4.0 - Sliding without overlap: 2.0
   */
  private double getWindowExpansionFactor(RelNode input) {
    RelNode current = input;
    while (current instanceof Project) {
      current = ((Project) current).getInput();
    }

    if (current instanceof TableFunctionScan scan) {
      RexNode call = scan.getCall();
      if (call instanceof RexCall rexCall) {
        String funcName = rexCall.getOperator().getName().toUpperCase();
        if (funcName.contains("HOP") || funcName.contains("SLIDE")) {
          // Check for overlap - if slide < window size, use higher factor
          return SLIDING_WINDOW_OVERLAP_FACTOR;
        } else if (funcName.contains("SESSION")) {
          return SESSION_WINDOW_FACTOR;
        }
      }
    }
    return TUMBLE_WINDOW_FACTOR;
  }

  /** Checks if the input contains a window table function (TUMBLE, HOP, SESSION, CUMULATE). */
  private boolean isWindowTableFunction(RelNode rel) {
    RelNode current = rel;
    while (current instanceof Project) {
      current = ((Project) current).getInput();
    }
    return current instanceof TableFunctionScan;
  }

  // ==================== Window (Over) ====================

  /** Window/Over aggregate returns input row count unchanged. */
  public Double getRowCount(Window rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  // ==================== TableFunctionScan ====================

  /** TableFunctionScan - for window functions, row count equals input. */
  public Double getRowCount(TableFunctionScan rel, RelMetadataQuery mq) {
    if (rel.getInputs().isEmpty()) {
      return rel.estimateRowCount(mq);
    }
    // Window table functions preserve input row count
    Double inputRowCount = mq.getRowCount(rel.getInput(0));
    return inputRowCount != null ? inputRowCount : rel.estimateRowCount(mq);
  }

  // ==================== Join ====================

  @Override
  public Double getRowCount(Join rel, RelMetadataQuery mq) {
    Double leftRowCount = mq.getRowCount(rel.getLeft());
    Double rightRowCount = mq.getRowCount(rel.getRight());

    if (leftRowCount == null || rightRowCount == null) {
      return null;
    }

    // Handle EnumerableNestedLoopJoin specially
    if (rel instanceof EnumerableNestedLoopJoin) {
      double rowCount = leftRowCount * rightRowCount;
      rowCount = rowCount + 2 * leftRowCount;
      return rowCount / 100;
    }

    // Get join info for equi-join analysis
    var joinInfo = rel.analyzeCondition();
    RexNode condition = rel.getCondition();

    // Calculate inner join row count
    double innerJoinRowCount;

    if (!joinInfo.leftKeys.isEmpty()) {
      // Equi-join: use NDV-based estimation
      innerJoinRowCount =
          estimateEquiJoinRowCount(
              rel, leftRowCount, rightRowCount, joinInfo.leftKeys, joinInfo.rightKeys, mq);

      // Apply selectivity for non-equi conditions
      if (joinInfo.nonEquiConditions != null && !joinInfo.nonEquiConditions.isEmpty()) {
        for (RexNode nonEquiCond : joinInfo.nonEquiConditions) {
          Double selectivity = mq.getSelectivity(rel, nonEquiCond);
          if (selectivity != null) {
            innerJoinRowCount *= selectivity;
          }
        }
      }
    } else {
      // Non-equi join: use selectivity-based estimation
      Double selectivity = mq.getSelectivity(rel, condition);
      if (selectivity == null) {
        selectivity = RelMdUtil.guessSelectivity(condition);
      }
      innerJoinRowCount = leftRowCount * rightRowCount * selectivity;
    }

    // Adjust based on join type
    return adjustForJoinType(rel.getJoinType(), leftRowCount, rightRowCount, innerJoinRowCount);
  }

  /**
   * Estimate equi-join row count using NDV. Formula: leftRowCount * rightRowCount * (1 /
   * max(leftNdv, rightNdv))
   */
  private double estimateEquiJoinRowCount(
      Join rel,
      double leftRowCount,
      double rightRowCount,
      ImmutableIntList leftKeys,
      ImmutableIntList rightKeys,
      RelMetadataQuery mq) {

    // Convert key lists to bitsets for NDV query
    ImmutableBitSet leftKeySet = ImmutableBitSet.of(leftKeys);
    ImmutableBitSet rightKeySet = ImmutableBitSet.of(rightKeys);

    // Get NDV for join keys
    Double leftNdv = null;
    Double rightNdv = null;

    try {
      leftNdv = mq.getDistinctRowCount(rel.getLeft(), leftKeySet, null);
      rightNdv = mq.getDistinctRowCount(rel.getRight(), rightKeySet, null);
    } catch (Exception e) {
      // Ignore
    }

    if (leftNdv == null) leftNdv = leftRowCount;
    if (rightNdv == null) rightNdv = rightRowCount;

    // Equi-join selectivity: 1 / max(leftNdv, rightNdv)
    double maxNdv = Math.max(leftNdv, rightNdv);
    double selectivity = maxNdv > 0 ? Math.min(1.0, 1.0 / maxNdv) : 1.0;

    return leftRowCount * rightRowCount * selectivity;
  }

  /**
   * Adjust row count based on join type. - INNER: innerJoinRowCount - LEFT: max(leftRowCount,
   * innerJoinRowCount) - RIGHT: max(rightRowCount, innerJoinRowCount) - FULL: max(left, inner) +
   * max(right, inner) - inner
   */
  private double adjustForJoinType(
      JoinRelType joinType, double leftRowCount, double rightRowCount, double innerJoinRowCount) {

    switch (joinType) {
      case INNER:
        return innerJoinRowCount;
      case LEFT:
        return Math.max(leftRowCount, innerJoinRowCount);
      case RIGHT:
        return Math.max(rightRowCount, innerJoinRowCount);
      case FULL:
        return Math.max(leftRowCount, innerJoinRowCount)
            + Math.max(rightRowCount, innerJoinRowCount)
            - innerJoinRowCount;
      case SEMI:
      case ANTI:
        // Semi/Anti join: at most left row count
        return leftRowCount * RelMdUtil.guessSelectivity(null);
      default:
        return innerJoinRowCount;
    }
  }

  // ==================== Correlate ====================

  /**
   * Correlate (lateral join) estimation. For temporal joins (with Snapshot), row count approximates
   * stream side.
   */
  public Double getRowCount(Correlate rel, RelMetadataQuery mq) {
    Double leftRowCount = mq.getRowCount(rel.getLeft());
    if (leftRowCount == null) {
      return null;
    }

    // Temporal join pattern: row count ≈ left (stream) side
    if (containsSnapshot(rel.getRight())) {
      return leftRowCount * 0.9;
    }

    // Regular correlate: estimate based on right side
    Double rightRowCount = mq.getRowCount(rel.getRight());
    if (rightRowCount == null) {
      return leftRowCount;
    }

    // Conservative: left * sqrt(right)
    return leftRowCount * Math.sqrt(rightRowCount);
  }

  /** Checks if the RelNode tree contains a Snapshot (temporal join pattern). */
  private boolean containsSnapshot(RelNode rel) {
    if (rel instanceof Snapshot) {
      return true;
    }
    for (RelNode input : rel.getInputs()) {
      if (containsSnapshot(input)) {
        return true;
      }
    }
    if (rel instanceof SingleRel singleRel) {
      return containsSnapshot(singleRel.getInput());
    }
    return false;
  }

  // ==================== Snapshot ====================

  /** Snapshot: row count equals input (point-in-time view). */
  public Double getRowCount(Snapshot rel, RelMetadataQuery mq) {
    return mq.getRowCount(rel.getInput());
  }

  // ==================== Set Operations ====================

  /** Union: sum of all inputs. */
  public Double getRowCount(Union rel, RelMetadataQuery mq) {
    double rowCount = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double inputRowCount = mq.getRowCount(input);
      if (inputRowCount == null) {
        return null;
      }
      rowCount += inputRowCount;
    }
    // UNION DISTINCT may have duplicates removed
    if (!rel.all) {
      rowCount = rowCount * 0.9;
    }
    return rowCount;
  }

  /** Intersect: minimum of all inputs. */
  public Double getRowCount(Intersect rel, RelMetadataQuery mq) {
    double minRowCount = Double.MAX_VALUE;
    for (RelNode input : rel.getInputs()) {
      Double inputRowCount = mq.getRowCount(input);
      if (inputRowCount == null) {
        return null;
      }
      minRowCount = Math.min(minRowCount, inputRowCount);
    }
    return minRowCount == Double.MAX_VALUE ? null : minRowCount;
  }

  /** Minus (EXCEPT): at most first input, reduced by overlap with others. */
  public Double getRowCount(Minus rel, RelMetadataQuery mq) {
    if (rel.getInputs().isEmpty()) {
      return null;
    }
    Double firstInputRowCount = mq.getRowCount(rel.getInput(0));
    if (firstInputRowCount == null) {
      return null;
    }
    // Conservative: reduce by estimated overlap
    double factor = 1.0;
    for (int i = 1; i < rel.getInputs().size(); i++) {
      Double inputRowCount = mq.getRowCount(rel.getInput(i));
      if (inputRowCount != null && firstInputRowCount > 0) {
        factor *= Math.max(0.5, 1.0 - inputRowCount / firstInputRowCount);
      }
    }
    return firstInputRowCount * factor;
  }

  // ==================== Dispatcher ====================

  @Override
  public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof TableScan scan) {
      return getRowCount(scan, mq);
    }
    if (rel instanceof Values values) {
      return getRowCount(values, mq);
    }
    if (rel instanceof Project project) {
      return getRowCount(project, mq);
    }
    if (rel instanceof Filter filter) {
      return getRowCount(filter, mq);
    }
    if (rel instanceof Calc calc) {
      return getRowCount(calc, mq);
    }
    if (rel instanceof Exchange exchange) {
      return getRowCount(exchange, mq);
    }
    if (rel instanceof Sort sort) {
      return getRowCount(sort, mq);
    }
    if (rel instanceof Aggregate aggregate) {
      return getRowCount(aggregate, mq);
    }
    if (rel instanceof Window window) {
      return getRowCount(window, mq);
    }
    if (rel instanceof TableFunctionScan tableFunctionScan) {
      return getRowCount(tableFunctionScan, mq);
    }
    if (rel instanceof Join join) {
      return getRowCount(join, mq);
    }
    if (rel instanceof Correlate correlate) {
      return getRowCount(correlate, mq);
    }
    if (rel instanceof Snapshot snapshot) {
      return getRowCount(snapshot, mq);
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
    return super.getRowCount(rel, mq);
  }

  // ==================== Static Utility Methods ====================

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
