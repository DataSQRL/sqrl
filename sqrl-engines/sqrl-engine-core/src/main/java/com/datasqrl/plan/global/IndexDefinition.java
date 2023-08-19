/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.function.IndexType;
import com.google.common.collect.ContiguousSet;
import lombok.Value;

import java.util.List;
import java.util.stream.Collectors;

@Value
public class IndexDefinition {

  public static final String INDEX_NAME = "_index_";

  String tableId;
  List<Integer> columns;
  List<String> columnNames;
  IndexType type;

  public IndexDefinition(String tableId, List<Integer> columns, List<String> allFieldNames,
      IndexType type) {
    this.tableId = tableId;
    this.columns = columns;
    this.columnNames = columns.stream().map(allFieldNames::get)
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
        fieldNames, IndexType.BTREE);
  }

}
