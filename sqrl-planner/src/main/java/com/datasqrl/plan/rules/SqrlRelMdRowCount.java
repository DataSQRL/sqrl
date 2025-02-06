/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import java.util.stream.IntStream;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;

public class SqrlRelMdRowCount extends RelMdRowCount
    implements BuiltInMetadata.RowCount.Handler {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new SqrlRelMdRowCount());

  @Override
public Double getRowCount(Join rel, RelMetadataQuery mq) {
    double rowCount = super.getRowCount(rel, mq);
    if (rel instanceof EnumerableNestedLoopJoin) {
      rowCount = rowCount + 2 * mq.getRowCount(rel.getLeft());
      //Undo the factor 10 penalty from EnumerableNestedLoopJoin
      rowCount = rowCount / 100;
    }
    return rowCount;
  }

  @Override
public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
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

  public static Double getRowCount(PhysicalRelationalTable table,
                                   QueryIndexSummary constraints) {
    var equalCols = constraints.getEqualityColumns();
    if (IntStream.of(table.getPrimaryKey().asArray()).allMatch(equalCols::contains)) {
      return 1.0;
    }
    return getRowCount(table) * SqrlRelMdSelectivity.getSelectivity(table, constraints);
  }

  public static Double getRowCount(ScriptRelationalTable table) {
    return table.getTableStatistic().getRowCount();
  }

}
