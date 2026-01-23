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

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.util.StreamUtil;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/** This class keeps track of the column indexes that are part of the primary key. */
@ToString
@AllArgsConstructor
public class PrimaryKeyMap implements Serializable {

  public static final PrimaryKeyMap UNDEFINED = new PrimaryKeyMap();
  @Singular final List<ColumnSet> columns;
  @Getter final boolean undefined;

  public PrimaryKeyMap(List<ColumnSet> columns) {
    var allIndexes = columns.stream().flatMap(ColumnSet::stream).toList();
    checkArgument(allIndexes.size() == Set.copyOf(allIndexes).size(), "Duplicate column indexes");
    this.columns = columns;
    this.undefined = false;
  }

  private PrimaryKeyMap() {
    this.columns = List.of();
    this.undefined = true;
  }

  public static PrimaryKeyMap of(List<Integer> pks) {
    return new PrimaryKeyMap(pks.stream().map(Set::of).map(ColumnSet::new).toList());
  }

  public static PrimaryKeyMap none() {
    return new PrimaryKeyMap(List.of());
  }

  public static PrimaryKeyMap of(int[] pks) {
    return of(IntStream.of(pks).boxed().collect(Collectors.toList()));
  }

  public static Builder build() {
    return new Builder();
  }

  public boolean isDefined() {
    return !isUndefined();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var that = (PrimaryKeyMap) o;
    return undefined == that.undefined && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, undefined);
  }

  public ColumnSet get(int position) {
    checkArgument(
        position >= 0 && position < columns.size(),
        "Primary key index out of bounds: %s",
        position);
    return columns.get(position);
  }

  public Optional<ColumnSet> getForIndex(int index) {
    return StreamUtil.getOnlyElement(columns.stream().filter(col -> col.contains(index)));
  }

  public int getLength() {
    if (undefined) {
      return -1;
    }
    return columns.size();
  }

  public boolean isSimple() {
    if (undefined) {
      return false;
    }
    return columns.stream().allMatch(ColumnSet::isSimple);
  }

  public List<ColumnSet> asList() {
    if (undefined) {
      return null;
    }
    return new ArrayList<>(columns);
  }

  /**
   * Note: only call if you are sure the primary key is simple (e.g. after calling {@link
   * #makeSimple(RelDataType)}.
   *
   * @return the primary key indexes as a list
   */
  public List<Integer> asSimpleList() {
    checkArgument(isSimple(), "Not a simple primary key");
    return columns.stream().map(ColumnSet::getOnly).toList();
  }

  public PrimaryKeyMap makeSimple(RelDataType rowType) {
    if (undefined) {
      return this;
    }
    return PrimaryKeyMap.of(columns.stream().map(colSet -> colSet.pickBest(rowType)).toList());
  }

  /**
   * Note: only call on defined primary keys
   *
   * @param indexes
   * @return whether all primary key components are covered (i.e. identical to) an index from the
   *     provided set.
   */
  public boolean coveredBy(Set<Integer> indexes) {
    if (undefined) return false;
    for (ColumnSet columnSet : columns) {
      if (!columnSet.containsAny(indexes)) return false;
    }
    return true;
  }

  public List<ColumnSet> asSubList(int length) {
    checkArgument(length <= getLength());
    return new ArrayList<>(columns.subList(0, length));
  }

  public PrimaryKeyMap remap(IndexMap remap) {
    return new PrimaryKeyMap(
        columns.stream().map(c -> c.remap(remap)).collect(Collectors.toList()));
  }

  public Optional<ColumnSet> find(int index) {
    return columns.stream().filter(c -> c.indexes.contains(index)).findFirst();
  }

  public Builder toBuilder() {
    checkArgument(!isUndefined(), "Cannot build on undefined primary key");
    var builder = build();
    columns.forEach(builder::add);
    return builder;
  }

  public record ColumnSet(Set<Integer> indexes) {

    public ColumnSet {
      checkArgument(!indexes.isEmpty());
    }

    public boolean isSimple() {
      return indexes.size() == 1;
    }

    public int getOnly() {
      checkArgument(isSimple());
      return Iterables.getOnlyElement(indexes);
    }

    public boolean isEmpty() {
      return indexes.isEmpty();
    }

    public ColumnSet intersect(ColumnSet other) {
      Set<Integer> intersection = new HashSet<>(indexes);
      intersection.retainAll(other.indexes);
      return new ColumnSet(intersection);
    }

    public boolean contains(int index) {
      return indexes.contains(index);
    }

    public boolean containsAny(Collection<Integer> colIndexes) {
      return !Collections.disjoint(indexes, colIndexes);
    }

    public boolean containsAny(ColumnSet other) {
      return containsAny(other.indexes);
    }

    private Stream<Integer> stream() {
      return indexes.stream();
    }

    public int pickBest(RelDataType rowType) {
      var fields = rowType.getFieldList();
      var fieldCount = fields.size();

      var validIndexes =
          indexes.stream().filter(idx -> idx >= 0 && idx < fieldCount).sorted().toList();

      // Try to pick the first not-null
      var nonNullPk =
          validIndexes.stream().filter(idx -> !fields.get(idx).getType().isNullable()).findFirst();

      // Otherwise, pick the first valid index
      return nonNullPk.orElseGet(
          () -> validIndexes.stream().findFirst().orElseThrow(() -> primaryKeyError(fields)));
    }

    public ColumnSet remap(IndexMap remap) {
      Set<Integer> result =
          indexes.stream()
              .map(remap::mapUnsafe)
              .filter(i -> i >= 0)
              .collect(Collectors.toUnmodifiableSet());
      checkArgument(
          !result.isEmpty(), "Mapping does not preserve any columns [%s]: %s", indexes, remap);
      return new ColumnSet(result);
    }

    private IllegalArgumentException primaryKeyError(List<RelDataTypeField> fields) {
      var fieldNames = fields.stream().map(RelDataTypeField::getName).toList();

      return new IllegalArgumentException(
          "Primary key index out of bounds. indexes=%s fields=%s".formatted(indexes, fieldNames));
    }
  }

  public static class Builder {

    final List<ColumnSet> columns;

    private Builder() {
      columns = new ArrayList<>();
    }

    private Builder(int length) {
      columns = Arrays.asList(new ColumnSet[length]);
    }

    public Builder add(ColumnSet column) {
      columns.add(column);
      return this;
    }

    public Builder add(Set<Integer> indexes) {
      columns.add(new ColumnSet(Set.copyOf(indexes)));
      return this;
    }

    public Builder add(int column) {
      columns.add(new ColumnSet(Set.of(column)));
      return this;
    }

    public Builder addAll(List<ColumnSet> columns) {
      this.columns.addAll(columns);
      return this;
    }

    public Builder addAllNotOverlapping(List<ColumnSet> columns) {
      columns.stream()
          .filter(colSet -> this.columns.stream().noneMatch(col -> col.containsAny(colSet)))
          .forEach(this.columns::add);
      return this;
    }

    public PrimaryKeyMap build() {
      checkArgument(columns.stream().noneMatch(Objects::isNull));
      return new PrimaryKeyMap(columns);
    }
  }
}
