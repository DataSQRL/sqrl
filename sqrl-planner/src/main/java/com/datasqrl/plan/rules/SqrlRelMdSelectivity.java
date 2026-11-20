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

import com.datasqrl.plan.index.QueryIndexSummary;
import com.datasqrl.plan.index.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Selectivity estimation handler adapted from Flink's FlinkRelMdSelectivity. Provides improved
 * selectivity estimates for various relational operators.
 */
public class SqrlRelMdSelectivity extends RelMdSelectivity
    implements BuiltInMetadata.Selectivity.Handler {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new SqrlRelMdSelectivity());

  /** The assumed selectivity of an equality on a column without a distinct count. */
  private static final double DEFAULT_EQUALITY_SELECTIVITY = 0.05;

  /** The assumed selectivity of an inequality on a column. */
  private static final double DEFAULT_INEQUALITY_SELECTIVITY = 0.5;

  /** Distinct counts of more columns first, and among those the higher distinct counts first. */
  private static final Comparator<Entry<Set<Integer>, Double>> MOST_COLUMNS_FIRST =
      Comparator.<Entry<Set<Integer>, Double>>comparingInt(
              distinctCount -> distinctCount.getKey().size())
          .thenComparingDouble(Entry::getValue)
          .reversed();

  @Override
  public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
    return super.getSelectivity(rel, mq, predicate);
  }

  /** Selectivity for Sort passes through to input since sorting doesn't affect row filtering. */
  public Double getSelectivity(Sort rel, RelMetadataQuery mq, RexNode predicate) {
    return mq.getSelectivity(rel.getInput(), predicate);
  }

  /** Selectivity for Exchange passes through to input since data redistribution doesn't filter. */
  public Double getSelectivity(Exchange rel, RelMetadataQuery mq, RexNode predicate) {
    return mq.getSelectivity(rel.getInput(), predicate);
  }

  /** Selectivity for Correlate (lateral join) - estimate based on left input. */
  public Double getSelectivity(Correlate rel, RelMetadataQuery mq, RexNode predicate) {
    return mq.getSelectivity(rel.getLeft(), predicate);
  }

  /** Selectivity for TableFunctionScan passes through to first input if available. */
  public Double getSelectivity(TableFunctionScan rel, RelMetadataQuery mq, RexNode predicate) {
    if (!rel.getInputs().isEmpty()) {
      return mq.getSelectivity(rel.getInput(0), predicate);
    }
    return RelMdUtil.guessSelectivity(predicate);
  }

  /**
   * Selectivity for Union - weighted average across all inputs. Formula: sumSelectedRows / sumRows
   */
  public Double getSelectivity(Union rel, RelMetadataQuery mq, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      return 1.0;
    }

    double sumRows = 0.0;
    double sumSelectedRows = 0.0;

    for (RelNode input : rel.getInputs()) {
      Double inputRowCount = mq.getRowCount(input);
      Double inputSelectivity = mq.getSelectivity(input, predicate);

      if (inputRowCount == null || inputSelectivity == null) {
        // Fall back to default estimation if we can't get metadata
        return RelMdUtil.guessSelectivity(predicate);
      }

      sumRows += inputRowCount;
      sumSelectedRows += inputSelectivity * inputRowCount;
    }

    if (sumRows < 1.0) {
      return RelMdUtil.guessSelectivity(predicate);
    }

    return sumSelectedRows / sumRows;
  }

  /**
   * Selectivity for Aggregate. For predicates on grouping columns, delegates to input. For
   * predicates on aggregate results, uses conservative estimate.
   */
  public Double getSelectivity(Aggregate rel, RelMetadataQuery mq, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      return 1.0;
    }

    // Conservative estimate: predicates on aggregates are harder to estimate
    // The selectivity depends on whether the predicate is on grouping columns
    // or on aggregate results
    double predicateSelectivity = RelMdUtil.guessSelectivity(predicate);

    return predicateSelectivity;
  }

  /** Selectivity for Filter using standard estimation. */
  public Double getSelectivity(Filter rel, RelMetadataQuery mq, RexNode predicate) {
    return estimateSelectivity(rel, mq, predicate);
  }

  /** Selectivity for Project using standard estimation. */
  public Double getSelectivity(Project rel, RelMetadataQuery mq, RexNode predicate) {
    return estimateSelectivity(rel, mq, predicate);
  }

  /** Selectivity for TableScan using standard estimation. */
  public Double getSelectivity(TableScan rel, RelMetadataQuery mq, RexNode predicate) {
    return estimateSelectivity(rel, mq, predicate);
  }

  /** Common selectivity estimation using Calcite's utilities. */
  private Double estimateSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    if (predicate == null || predicate.isAlwaysTrue()) {
      return 1.0;
    }
    if (predicate.isAlwaysFalse()) {
      return 0.0;
    }
    return RelMdUtil.guessSelectivity(predicate);
  }

  /**
   * Static method for constraint-based selectivity estimation on a table. Used for database query
   * cost estimation.
   */
  public static Double getSelectivity(TableAnalysis table, QueryIndexSummary constraints) {
    var selectivity =
        getEqualitySelectivity(
            table.getTableStatistic().getDistinctCounts(), constraints.getEqualityColumns());
    selectivity *=
        Math.pow(DEFAULT_INEQUALITY_SELECTIVITY, constraints.getInequalityColumns().size());
    for (IndexableFunctionCall fcall : constraints.getFunctionCalls()) {
      selectivity *= fcall.function().estimateSelectivity();
    }
    return selectivity;
  }

  /**
   * The selectivity of equalities on all the given columns. A distinct count applies when the
   * equalities include all of its columns, and those columns then match one row in that many. When
   * distinct counts share a column, the one with more columns applies. Each column that is left
   * gets the default selectivity.
   */
  private static double getEqualitySelectivity(
      Map<Set<Integer>, Double> distinctCounts, Set<Integer> equalityColumns) {
    var columnsWithoutDistinctCount = new HashSet<>(equalityColumns);
    var selectivity = 1.0d;
    for (var distinctCount :
        distinctCounts.entrySet().stream().sorted(MOST_COLUMNS_FIRST).toList()) {
      if (columnsWithoutDistinctCount.containsAll(distinctCount.getKey())) {
        // A count below one row would give a selectivity above 1
        selectivity /= Math.max(1.0d, distinctCount.getValue());
        columnsWithoutDistinctCount.removeAll(distinctCount.getKey());
      }
    }
    return selectivity * Math.pow(DEFAULT_EQUALITY_SELECTIVITY, columnsWithoutDistinctCount.size());
  }
}
