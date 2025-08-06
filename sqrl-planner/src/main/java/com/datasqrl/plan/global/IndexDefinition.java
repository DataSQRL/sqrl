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
package com.datasqrl.plan.global;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class IndexDefinition implements Comparable<IndexDefinition> {

  public static final String INDEX_NAME = "_index_";

  String tableName;
  List<Integer> columns;
  List<String> columnNames;
  int partitionOffset;
  IndexType type;

  public IndexDefinition(
      String tableName,
      List<Integer> columns,
      List<String> allFieldNames,
      int partitionOffset,
      IndexType type) {
    Preconditions.checkArgument(
        type.isPartitioned() ^ partitionOffset < 0,
        "Index must be partitioned XOR partition offset must be negative: %s | %s",
        type,
        partitionOffset);
    Preconditions.checkArgument(
        partitionOffset <= columns.size(),
        "Invalid partition offset: %s | %s",
        partitionOffset,
        columns.size());
    this.tableName = tableName;
    this.columns = columns;
    this.partitionOffset = partitionOffset;
    this.columnNames = columns.stream().map(allFieldNames::get).collect(Collectors.toList());
    this.type = type;
  }

  private IndexDefinition(
      String tableName, List<Integer> columns, List<String> columnNames, IndexType type) {
    this.tableName = tableName;
    this.columns = columns;
    this.columnNames = columnNames;
    this.partitionOffset = -1;
    this.type = type;
  }

  public String getName() {
    return tableName
        + "_"
        + type.name().toLowerCase()
        + "_"
        + columns.stream().map(i -> "c" + i).collect(Collectors.joining());
  }

  public static IndexDefinition getPrimaryKeyIndex(
      String tableId, List<Integer> primaryKeys, List<String> pkNames) {
    return new IndexDefinition(tableId, primaryKeys, pkNames, IndexType.BTREE);
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
