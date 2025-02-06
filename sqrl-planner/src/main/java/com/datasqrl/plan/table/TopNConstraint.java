/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;

import com.datasqrl.plan.util.IndexMap;
import com.google.common.base.Preconditions;

import lombok.NonNull;
import lombok.Value;

@Value
public class TopNConstraint implements PullupOperator {

  public static TopNConstraint EMPTY = new TopNConstraint(List.of(), false, RelCollations.EMPTY,
      Optional.empty(),false);

  @NonNull List<Integer> partition; //First, we partition in the input relation
  boolean distinct; //second, we select distinct rows if true [All pk columns not in the partition indexes are SELECT DISTINCT columns]
  @NonNull RelCollation collation; //third, we sort it
  @NonNull Optional<Integer> limit; //fourth, we limit the result

  boolean isTimestampOrder; //true, if the first order clause in the collation orders by timestamp

  public TopNConstraint(List<Integer> partition, boolean distinct, RelCollation collation,
      Optional<Integer> limit, boolean isTimestampOrder) {
    this.partition = partition;
    this.distinct = distinct;
    this.collation = collation;
    this.limit = limit;
    this.isTimestampOrder = isTimestampOrder;
    Preconditions.checkArgument(isEmpty() || distinct || hasLimit());
    Preconditions.checkArgument(isEmpty() || !distinct || hasPartition() || !hasCollation());
  }

  public boolean isEmpty() {
    return !distinct && !hasLimit() && !hasCollation() && !hasPartition();
  }

  public boolean hasPartition() {
    return !partition.isEmpty();
  }

  public boolean hasCollation() {
    return !collation.getFieldCollations().isEmpty();
  }

  public boolean hasLimit() {
    return limit.isPresent();
  }

  public boolean hasLimit(int limit) {
    return limit == this.limit.orElse(-1);
  }

  public int getLimit() {
    Preconditions.checkArgument(hasLimit());
    return limit.get();
  }

  public TopNConstraint remap(IndexMap map) {
    if (isEmpty()) {
      return this;
    }
    var newCollation = map.map(collation);
    List<Integer> newPartition = partition.stream().map(i -> map.map(i))
        .collect(Collectors.toList());
    return new TopNConstraint(newPartition, distinct, newCollation, limit, isTimestampOrder);
  }

  public List<Integer> getIndexes() {
    return Stream.concat(collation.getFieldCollations().stream().map(RelFieldCollation::getFieldIndex),
        partition.stream()).collect(Collectors.toList());
  }


  public static TopNConstraint makeDeduplication(List<Integer> partitionByIndexes,
      int timestampIndex) {
    var collation = RelCollations.of(
        new RelFieldCollation(timestampIndex, RelFieldCollation.Direction.DESCENDING,
            RelFieldCollation.NullDirection.LAST));
    return new TopNConstraint(partitionByIndexes, false, collation, Optional.of(1),
        true);
  }

  public boolean isDeduplication() {
    //Deduplication may not have a partition for global/single row tables, e.g. after a global aggregation
    return !distinct && isTimestampOrder && hasLimit(1);
  }

  public boolean isDescendingDeduplication() {
    //Timestamps cannot be null, so we don't have to check for null direction
    return isDeduplication() && collation.getFieldCollations().get(0).getDirection() ==
        RelFieldCollation.Direction.DESCENDING && collation.getFieldCollations().size()==1;
  }


}
