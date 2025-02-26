/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

@FunctionalInterface
public interface IndexMap {

  final IndexMap IDENTITY = idx -> idx;

  /**
   * Maps the given index. Returns a negative integer if the given index does not have a mapping.
   *
   * @param index
   * @return
   */
  int mapUnsafe(int index);

  default int map(int index) {
    int target = mapUnsafe(index);
    Preconditions.checkArgument(target >= 0, "Invalid index: %s", index);
    return target;
  }

  default RexNode map(@NonNull RexNode node, @NonNull RelDataType inputType) {
    return map(node, inputType.getFieldList());
  }

  default RexNode map(@NonNull RexNode node, @NonNull List<RelDataTypeField> inputFields) {
    return node.accept(new RexIndexMapShuttle(this, inputFields));
  }

  default RelCollation map(RelCollation collation) {
    return RelCollations.of(
        collation.getFieldCollations().stream()
            .map(fc -> fc.withFieldIndex(this.map(fc.getFieldIndex())))
            .collect(Collectors.toList()));
  }

  @AllArgsConstructor
  class RexIndexMapShuttle extends RexShuttle {

    private final IndexMap map;
    private final List<RelDataTypeField> inputFields;

    @Override
    public RexNode visitInputRef(RexInputRef input) {
      int inputIdx = map.map(input.getIndex());
      Preconditions.checkArgument(inputIdx >= 0 && inputIdx < inputFields.size());
      return new RexInputRef(inputIdx, inputFields.get(inputIdx).getType());
    }
  }

  @Value
  class Pair {

    int source;
    int target;
  }

  static IndexMap of(final Map<Integer, Integer> mapping) {
    return idx -> {
      Integer map = mapping.get(idx);
      return map != null ? map : -1;
    };
  }

  static IndexMap of(final List<Integer> references) {
    final Map<Integer, Integer> mapping = new HashMap<>();
    for (int target = 0; target < references.size(); target++) {
      int source = references.get(target);
      if (source >= 0) mapping.putIfAbsent(source, target);
    }
    return of(mapping);
  }

  static IndexMap singleton(final int source, final int target) {
    return idx -> {
      return idx == source ? target : -1;
    };
  }
}
