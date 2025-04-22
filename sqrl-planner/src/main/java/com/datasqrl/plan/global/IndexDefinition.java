/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import java.util.List;
import java.util.stream.Collectors;

import com.datasqrl.function.IndexType;
import com.google.common.base.Preconditions;

import lombok.Value;

@Value
public class IndexDefinition implements Comparable<IndexDefinition> {

  public static final String INDEX_NAME = "_index_";

  String tableId;
  List<Integer> columns;
  List<String> columnNames;
  int partitionOffset;
  IndexType type;

  public IndexDefinition(String tableId, List<Integer> columns, List<String> allFieldNames,
      int partitionOffset, IndexType type) {
    Preconditions.checkArgument(type.isPartitioned() ^ partitionOffset < 0, "Index must be partitioned XOR partition offset must be negative: %s | %s", type, partitionOffset);
    Preconditions.checkArgument(partitionOffset<=columns.size(), "Invalid partition offset: %s | %s", partitionOffset, columns.size());
    this.tableId = tableId;
    this.columns = columns;
    this.partitionOffset = partitionOffset;
    this.columnNames = columns.stream().map(allFieldNames::get)
        .collect(Collectors.toList());
    this.type = type;
  }

  public String getName() {
    return tableId + "_" + type.name().toLowerCase() + "_" +
        columns.stream().map(i -> "c" + i).collect(Collectors.joining());
  }

  public static IndexDefinition getPrimaryKeyIndex(String tableId, List<Integer> primaryKeys,
      List<String> fieldNames) {
    return new IndexDefinition(tableId, primaryKeys,
        fieldNames, -1, IndexType.BTREE);
  }

  public int numEqualityColumnsRequired() {
    if (type.requiresAllColumns()) {
        return columns.size();
    }
    if (type.isPartitioned()) {
        return partitionOffset;
    }
    return 0;
  }

  @Override
  public int compareTo(IndexDefinition o) {
    return getName().compareTo(o.getName());
  }
}
