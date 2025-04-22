/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.ListUtils;

import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.plan.util.PrimaryKeyMap;

import lombok.Value;

/**
 * TODO: Pullup sort orders through the logical plan and into the database (or discard if they no longer apply)
 */
@Value
public class SortOrder implements PullupOperator {

  public static SortOrder EMPTY = new SortOrder(RelCollations.EMPTY);

  final RelCollation collation;

  public boolean isEmpty() {
    return collation.getFieldCollations().isEmpty();
  }

  public SortOrder remap(IndexMap map) {
    if (isEmpty()) {
      return this;
    }
    var newCollation = RelCollations.of(collation.getFieldCollations().stream()
        .map(fc -> fc.withFieldIndex(map.map(fc.getFieldIndex()))).collect(Collectors.toList()));
    return new SortOrder(newCollation);
  }

  public RelBuilder addTo(RelBuilder relBuilder) {
    if (!isEmpty()) {
      relBuilder.sort(collation);
    }
    return relBuilder;
  }

  public SortOrder ifEmpty(SortOrder secondary) {
    if (isEmpty()) {
      return secondary;
    } else {
      return this;
    }
  }

  public SortOrder join(SortOrder right) {
    if (isEmpty()) {
      return right;
    }
    return new SortOrder(RelCollations.of(
        ListUtils.union(collation.getFieldCollations(), right.collation.getFieldCollations())));
  }

  public SortOrder ensurePrimaryKeyPresent(PrimaryKeyMap pk) {
    if (pk.isUndefined()) {
        return this;
    }
    List<Integer> pkIdx = new ArrayList<>(pk.asSimpleList()); //PK must be simple after post-processing
    collation.getFieldCollations().stream().map(fc -> fc.getFieldIndex()).forEach(pkIdx::remove);
    if (pkIdx.isEmpty()) {
      return this;
    }
    //Append remaining pk columns to end of collation
    List<RelFieldCollation> collations = new ArrayList<>(collation.getFieldCollations());
    for (Integer idx : pkIdx) {
      collations.add(new RelFieldCollation(idx));
    }
    return new SortOrder(RelCollations.of(collations));
  }

  public static SortOrder of(List<Integer> partition, RelCollation collation) {
    List<RelFieldCollation> collationList = new ArrayList<>();
    collationList.addAll(partition.stream()
        .map(idx -> new RelFieldCollation(idx, RelFieldCollation.Direction.ASCENDING,
            RelFieldCollation.NullDirection.LAST)).collect(Collectors.toList()));
    collationList.addAll(collation.getFieldCollations());
    return new SortOrder(RelCollations.of(collationList));
  }
}
