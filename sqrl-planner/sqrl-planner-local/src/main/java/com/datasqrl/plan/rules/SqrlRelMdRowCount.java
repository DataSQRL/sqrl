/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.table.ScriptRelationalTable;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.util.BuiltInMethod;

import java.util.Set;
import java.util.stream.IntStream;

public class SqrlRelMdRowCount extends RelMdRowCount
    implements BuiltInMetadata.RowCount.Handler {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.ROW_COUNT.method, new SqrlRelMdRowCount());

  public Double getRowCount(Join rel, RelMetadataQuery mq) {
    double rowCount = super.getRowCount(rel, mq);
    if (rel instanceof EnumerableNestedLoopJoin) {
      rowCount = rowCount + 2 * mq.getRowCount(rel.getLeft());
      //Undo the factor 10 penalty from EnumerableNestedLoopJoin
      rowCount = rowCount / 100;
    }
    return rowCount;
  }

  public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
    if (rel instanceof Join) {
      return getRowCount((Join) rel, mq);
    }
    if (rel instanceof Filter) {
      return getRowCount((Filter) rel, mq);
    }
    return super.getRowCount(rel, mq);
  }

  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    return RelMdUtil.estimateFilteredRows(rel.getInput(), rel.getCondition(), mq);
  }

  public static Double getRowCount(ScriptRelationalTable table,
                                   QueryIndexSummary constraints) {
    Set<Integer> equalCols = constraints.getEqualityColumns();
    if (IntStream.range(0, table.getNumPrimaryKeys()).allMatch(equalCols::contains)) {
      return 1.0;
    }
    return getRowCount(table) * SqrlRelMdSelectivity.getSelectivity(table, constraints);
  }

  public static Double getRowCount(ScriptRelationalTable table) {
    return table.getTableStatistic().getRowCount();
  }

}
