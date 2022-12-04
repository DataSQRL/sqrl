package com.datasqrl.plan.calcite.rules;

import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.global.IndexCall;
import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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

  public static Double getRowCount(VirtualRelationalTable table,
      List<IndexCall.IndexColumn> constraints) {
    Set<Integer> equalCols = constraints.stream()
        .filter(c -> c.getType() == IndexCall.CallType.EQUALITY)
        .map(IndexCall.IndexColumn::getColumnIndex).collect(Collectors.toSet());
    if (IntStream.range(0, table.getNumPrimaryKeys()).allMatch(idx -> equalCols.contains(idx))) {
      return 1.0;
    }
    return getRowCount(table) * SqrlRelMdSelectivity.getSelectivity(table, constraints);
  }

  public static Double getRowCount(VirtualRelationalTable table) {
    return table.getTableStatistic().getRowCount();
  }

}
