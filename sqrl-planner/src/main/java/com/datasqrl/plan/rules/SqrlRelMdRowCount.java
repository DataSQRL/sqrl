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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

public class SqrlRelMdRowCount extends RelMdRowCount implements BuiltInMetadata.RowCount.Handler {

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

    var tableAnalysis = lookupTableAnalysis(scan);
    if (tableAnalysis != null
        && tableAnalysis.getTableStatistic() != null
        && !tableAnalysis.getTableStatistic().isUnknown()) {
      return tableAnalysis.getTableStatistic().getRowCount();
    }
    return super.getRowCount(scan, mq);
  }

  @Nullable
  private TableAnalysis lookupTableAnalysis(TableScan scan) {
    var table = scan.getTable();
    if (table instanceof TableSourceTable sourceTable) {
      ObjectIdentifier tableId = sourceTable.contextResolvedTable().getIdentifier();
      var sourceTableAnalysis = tableLookup.lookupSourceTable(tableId);
      if (sourceTableAnalysis != null) {
        return sourceTableAnalysis;
      }
      return tableLookup.lookupView(tableId);
    }
    return tableLookup.lookupView(scan).orElse(null);
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
