package com.datasqrl.plan.util;

import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.Iterables;
import lombok.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.checkerframework.checker.units.qual.C;

/**
 * This class keeps track of the column indexes that are part of the primary key.
 */
@ToString
@AllArgsConstructor
public class PrimaryKeyMap implements Serializable {

  public static final PrimaryKeyMap UNDEFINED = new PrimaryKeyMap();
  @Singular
  final List<ColumnSet> columns;
  final boolean undefined;

  public PrimaryKeyMap(List<ColumnSet> columns) {
    List<Integer> allIndexes = columns.stream().flatMap(ColumnSet::stream)
        .collect(Collectors.toUnmodifiableList());
    Preconditions.checkArgument(allIndexes.size() == Set.copyOf(allIndexes).size(),
        "Duplicate column indexes");
    this.columns = columns;
    this.undefined = false;
  }

  private PrimaryKeyMap() {
    this.columns = List.of();
    this.undefined = true;
  }

  public static PrimaryKeyMap of(List<Integer> pks) {
    return new PrimaryKeyMap(
        pks.stream().map(Set::of).map(ColumnSet::new).collect(Collectors.toUnmodifiableList()));
  }

  public static PrimaryKeyMap none() {
    return new PrimaryKeyMap(List.of());
  }

  public static PrimaryKeyMap of(int[] pks) {
    return of(IntStream.of(pks).boxed().collect(Collectors.toList()));
  }

  public static PrimaryKeyMap of(List<String> names, RelDataType type) {
    return PrimaryKeyMap.of(names.stream()
        .map(name -> {
          int idx = CalciteUtil.indexOf(name, type);
          if (idx < 0) throw new IllegalArgumentException("Primary key column not found in table: " + name);
          return idx;
        }).collect(Collectors.toList()));
  }

  public static Builder build() {
    return new Builder();
  }

  public boolean isUndefined() {
    return undefined;
  }

  public boolean isDefined() {
    return !undefined;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrimaryKeyMap that = (PrimaryKeyMap) o;
    return undefined == that.undefined && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns, undefined);
  }

  public ColumnSet get(int position) {
    Preconditions.checkArgument(position >= 0 && position < columns.size(),
        "Primary key index out of bounds: %s", position);
    return columns.get(position);
  }

  public Optional<ColumnSet> getForIndex(int index) {
    return StreamUtil.getOnlyElement(columns.stream().filter(col -> col.contains(index)));
  }

  public int getLength() {
    if (undefined) return -1;
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

  public List<Integer> asSimpleList() {
    Preconditions.checkArgument(isSimple(), "Not a simple primary key");
    return columns.stream().map(ColumnSet::getOnly).collect(Collectors.toUnmodifiableList());
  }

  public List<ColumnSet> asSubList(int length) {
    Preconditions.checkArgument(length <= getLength());
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
    Preconditions.checkArgument(!isUndefined(), "Cannot build on undefined primary key");
    Builder builder = build();
    columns.forEach(builder::add);
    return builder;
  }

  @Value
  public static class ColumnSet {

    private Set<Integer> indexes;

    public ColumnSet(Set<Integer> indexes) {
      Preconditions.checkArgument(!indexes.isEmpty());
      this.indexes = indexes;
    }

    public boolean isSimple() {
      return indexes.size() == 1;
    }

    public int getOnly() {
      Preconditions.checkArgument(isSimple());
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
      List<RelDataTypeField> fields = rowType.getFieldList();
      //Try to pick the first not-null
      Optional<Integer> nonNullPk = indexes.stream()
          .filter(idx -> !fields.get(idx).getType().isNullable()).sorted().findFirst();
      //Otherwise, pick the first
      return nonNullPk.orElseGet(() -> indexes.stream().sorted().findFirst().get());

    }

    public ColumnSet remap(IndexMap remap) {
      Set<Integer> result = indexes.stream().map(remap::mapUnsafe).filter(i -> i >= 0)
          .collect(Collectors.toUnmodifiableSet());
      Preconditions.checkArgument(!result.isEmpty(),
          "Mapping does not preserve any columns [%s]: %s", indexes, remap);
      return new ColumnSet(result);
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
      Preconditions.checkArgument(columns.stream().noneMatch(Objects::isNull));
      return new PrimaryKeyMap(columns);
    }

  }
}