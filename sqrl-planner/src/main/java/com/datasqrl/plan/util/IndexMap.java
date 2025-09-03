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
package com.datasqrl.plan.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
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
    var target = mapUnsafe(index);
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
      var inputIdx = map.map(input.getIndex());
      Preconditions.checkArgument(inputIdx >= 0 && inputIdx < inputFields.size());
      return new RexInputRef(inputIdx, inputFields.get(inputIdx).getType());
    }
  }

  static IndexMap of(final Map<Integer, Integer> mapping) {
    return idx -> {
      var map = mapping.get(idx);
      return map != null ? map : -1;
    };
  }

  static IndexMap of(final List<Integer> references) {
    final Map<Integer, Integer> mapping = new HashMap<>();
    for (var target = 0; target < references.size(); target++) {
      int source = references.get(target);
      if (source >= 0) {
        mapping.putIfAbsent(source, target);
      }
    }
    return of(mapping);
  }

  static IndexMap singleton(final int source, final int target) {
    return idx -> {
      return idx == source ? target : -1;
    };
  }
}
