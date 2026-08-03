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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import org.apache.calcite.rel.RelFieldCollation.Direction;

@Value
public class IndexDefinition implements Comparable<IndexDefinition> {

  public static final String INDEX_NAME = "_index_";

  String tableName;
  List<Integer> columns;
  List<String> columnNames;
  List<Direction> directions;
  int partitionOffset;
  IndexType type;

  public IndexDefinition(
      String tableName,
      List<Integer> columns,
      List<String> allFieldNames,
      int partitionOffset,
      IndexType type) {
    this(
        tableName,
        columns,
        allFieldNames,
        partitionOffset,
        type,
        columns.stream().map(column -> Direction.ASCENDING).toList());
  }

  public IndexDefinition(
      String tableName,
      List<Integer> columns,
      List<String> allFieldNames,
      int partitionOffset,
      IndexType type,
      List<Direction> directions) {

    checkArgument(
        type.isPartitioned() ^ partitionOffset < 0,
        "Index must be partitioned XOR partition offset must be negative: %s | %s",
        type,
        partitionOffset);

    checkArgument(
        partitionOffset <= columns.size(),
        "Invalid partition offset: %s | %s",
        partitionOffset,
        columns.size());

    checkArgument(
        columns.size() == directions.size(),
        "Number of index column directions must match number of columns: %s | %s",
        columns.size(),
        directions.size());

    this.tableName = tableName;
    this.columns = columns;
    this.partitionOffset = partitionOffset;
    this.columnNames = columns.stream().map(allFieldNames::get).collect(Collectors.toList());
    this.type = type;
    this.directions = directions;
  }

  private IndexDefinition(
      String tableName,
      List<Integer> columns,
      List<String> columnNames,
      List<Direction> directions,
      IndexType type) {
    this.tableName = tableName;
    this.columns = columns;
    this.columnNames = columnNames;
    this.directions = directions;
    this.partitionOffset = -1;
    this.type = type;
  }

  public String getName() {
    return tableName
        + "_"
        + type.name().toLowerCase()
        + "_"
        + IntStream.range(0, columns.size())
            .mapToObj(i -> "c" + columns.get(i) + (directions.get(i).isDescending() ? "d" : ""))
            .collect(Collectors.joining());
  }

  public static IndexDefinition getPrimaryKeyIndex(
      String tableId, List<Integer> primaryKeys, List<String> pkNames) {
    return new IndexDefinition(
        tableId,
        primaryKeys,
        pkNames,
        primaryKeys.stream().map(column -> Direction.ASCENDING).toList(),
        IndexType.BTREE);
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
