/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.google.common.collect.ContiguousSet;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value
public class IndexDefinition {

  public static final String INDEX_NAME = "_index_";

  public enum Type {
    HASH, BTREE;

    public boolean hasStrictColumnOrder() {
      return this == HASH || this == BTREE;
    }

    public boolean requiresAllColumns() {
      return this == HASH;
    }

  }

  String tableId;
  List<Integer> columns;
  List<String> columnNames;
  Type type;

  public IndexDefinition(String tableId, List<Integer> columns, List<String> fieldNames,
      Type type) {
    this.tableId = tableId;
    this.columns = columns;
    this.columnNames = columns.stream().map(c -> fieldNames.get(c))
        .collect(Collectors.toList());
    this.type = type;
  }

  public String getName() {
    return tableId + "_" + type.name().toLowerCase() + "_" +
        columns.stream().map(i -> "c" + i).collect(Collectors.joining());
  }

  public static IndexDefinition getPrimaryKeyIndex(String tableId, int numPrimaryKeys,
      List<String> fieldNames) {
    return new IndexDefinition(tableId, ContiguousSet.closedOpen(0, numPrimaryKeys).asList(),
        fieldNames, Type.BTREE);
  }

}
