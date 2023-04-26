/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.plan.rules.SqrlRelMdRowCount;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public class IndexCall {

  public static final String INDEX_NAME = "_index_";

  VirtualRelationalTable table;
  Map<Integer, IndexColumn> indexColumns;
  double relativeFrequency;

  public static IndexCall of(@NonNull VirtualRelationalTable table,
      @NonNull List<IndexColumn> columns) {
    Preconditions.checkArgument(!columns.isEmpty());
    Map<Integer, IndexColumn> indexCols = new HashMap<>();
    columns.stream().filter(c -> c.getType() == CallType.EQUALITY)
        .distinct().forEach(c -> indexCols.put(c.columnIndex, c));
    columns.stream().filter(c -> c.getType() == CallType.COMPARISON)
        .filter(c -> !indexCols.containsKey(c.columnIndex))
        .distinct().forEach(c -> indexCols.put(c.columnIndex, c));
    return new IndexCall(table, indexCols, 1.0);
  }

  public static Optional<IndexCall> of(@NonNull VirtualRelationalTable table, RexNode filter,
      SqrlRexUtil rexUtil) {
    List<RexNode> conjunctions = rexUtil.getConjunctions(filter);
    List<IndexColumn> columns = new ArrayList<>();
    for (RexNode conj : conjunctions) {
      if (conj instanceof RexCall) {
        RexCall call = (RexCall) conj;
        Set<Integer> inputRefs = rexUtil.findAllInputRefs(call.getOperands());
        CallType indexType = null;
        if (call.isA(SqlKind.EQUALS)) {
          indexType = CallType.EQUALITY;
        } else if (call.isA(SqlKind.COMPARISON)) {
          indexType = CallType.COMPARISON;
        }
        if (inputRefs.size() == 1 && indexType != null) {
          columns.add(new IndexColumn(Iterables.getOnlyElement(inputRefs), indexType));
        }
      }
    }
    if (columns.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(IndexCall.of(table, columns));
    }
  }

  public double getCost(@NonNull IndexDefinition indexDef) {
    List<IndexColumn> coveredCols = new ArrayList<>();

    int i = 0;
    for (; i < indexDef.getColumns().size(); i++) {
      IndexColumn idxCol = indexColumns.get(indexDef.getColumns().get(i));
      boolean breaksEqualityChain = idxCol == null;
      if (idxCol != null) {
        if (idxCol.type.supportedBy(indexDef.getType())) {
          coveredCols.add(idxCol);
        } else {
          breaksEqualityChain = true;
        }
        if (idxCol.type != CallType.EQUALITY) {
          breaksEqualityChain = true;
        }
      }

      if (breaksEqualityChain && indexDef.getType().hasStrictColumnOrder()) {
        break;
      }
    }
    if (i < indexDef.getColumns().size() && indexDef.getType().requiresAllColumns()) {
      //This index requires a constraint on all columns to be invocable
      coveredCols = List.of();
    }
    return SqrlRelMdRowCount.getRowCount(table, coveredCols);
  }

  public Collection<IndexColumn> getColumns() {
    return indexColumns.values();
  }

  public Set<Integer> getColumnIndexes() {
    return indexColumns.keySet();
  }

  @Override
  public String toString() {
    return table.getNameId() +
        getColumns().stream().sorted().map(IndexColumn::getName).collect(Collectors.joining());
  }

  @Value
  public static class IndexColumn implements Comparable<IndexColumn> {

    int columnIndex;
    CallType type;

    public String getName() {
      return columnIndex + type.shortForm;
    }

    public IndexColumn withType(CallType type) {
      return new IndexColumn(columnIndex, type);
    }

    @Override
    public int compareTo(@NonNull IndexCall.IndexColumn o) {
      return Integer.compare(columnIndex, o.columnIndex);
    }
  }


  public enum CallType {

    EQUALITY("e"), COMPARISON("i");

    private final String shortForm;

    CallType(String shortForm) {
      this.shortForm = shortForm;
    }

    public boolean supportedBy(IndexDefinition.Type indexType) {
      switch (indexType) {
        case HASH:
          return this == IndexCall.CallType.EQUALITY;
        case BTREE:
          return this == IndexCall.CallType.EQUALITY || this == IndexCall.CallType.COMPARISON;
        default:
          throw new IllegalStateException(this.name());
      }
    }

  }

}
