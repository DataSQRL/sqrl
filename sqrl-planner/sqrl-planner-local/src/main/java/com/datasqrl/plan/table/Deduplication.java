/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import com.datasqrl.plan.util.IndexMap;
import lombok.Value;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.stream.Collectors;

@Value
public class Deduplication implements PullupOperator {

  public static Deduplication EMPTY = new Deduplication(List.of(), -1);

  final List<Integer> partitionByIndexes;
  final int timestampIndex;

  public boolean isEmpty() {
    return timestampIndex < 0;
  }

  public boolean hasPartition() {
    return !partitionByIndexes.isEmpty();
  }

  public Deduplication remap(IndexMap map) {
    if (this == EMPTY) {
      return EMPTY;
    }
    List<Integer> newPartition = partitionByIndexes.stream().map(i -> map.map(i))
        .collect(Collectors.toList());
    int newTimestampIndex = map.map(timestampIndex);
    return new Deduplication(newPartition, newTimestampIndex);
  }

  public RelBuilder addDedup(RelBuilder relBuilder) {
    RexBuilder rexB = relBuilder.getRexBuilder();
    throw new UnsupportedOperationException("Not yet implemented");
  }

}
