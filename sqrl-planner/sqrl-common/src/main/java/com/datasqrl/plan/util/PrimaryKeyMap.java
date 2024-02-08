package com.datasqrl.plan.util;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.ToString;

/**
 * This class keeps track of the column indexes that are part of the primary key.
 *
 * It implements {@link IndexMap} by mapping i to the i-th primary key column index.
 */
@EqualsAndHashCode
@ToString
@Builder
@AllArgsConstructor
public class PrimaryKeyMap implements IndexMap, Serializable {

  @Singular
  final List<Integer> pkIndexes;
  final boolean undefined;

  public PrimaryKeyMap(List<Integer> pkIndexes) {
    Preconditions.checkArgument(Set.copyOf(pkIndexes).size() == pkIndexes.size(),
            "Duplicate primary key index: %s", pkIndexes);
    this.pkIndexes = pkIndexes;
    this.undefined = false;
  }

  public static final PrimaryKeyMap UNDEFINED = new PrimaryKeyMap();

  private PrimaryKeyMap() {
    this.pkIndexes = List.of();
    this.undefined = true;
  }

  public boolean isUndefined() {
    return undefined;
  }

  @Override
  public int map(int index) {
    Preconditions.checkArgument(index>=0 && index<pkIndexes.size(),"Primary key index out of bounds: %s", index);
    return pkIndexes.get(index);
  }

  public int getLength() {
    return pkIndexes.size();
  }

  public List<Integer> asList() {
    if (undefined) return null;
    return new ArrayList<>(pkIndexes);
  }

  public int[] asArray() {
    if (undefined) return null;
    return pkIndexes.stream().mapToInt(Integer::intValue).toArray();
  }

  public PrimaryKeyMap remap(IndexMap remap) {
    return new PrimaryKeyMap(pkIndexes.stream().map(remap::map).collect(Collectors.toList()));
  }

  public boolean containsIndex(int index) {
    return pkIndexes.contains(index);
  }

  public PrimaryKeyMapBuilder toBuilder() {
    PrimaryKeyMapBuilder builder = builder();
    pkIndexes.forEach(builder::pkIndex);
    builder.undefined(this.undefined);
    return builder;
  }

  public static PrimaryKeyMap of(List<Integer> pks) {
    return new PrimaryKeyMap(List.copyOf(pks));
  }

  public static PrimaryKeyMap of(int[] pks) {
    return new PrimaryKeyMap(IntStream.of(pks).boxed().collect(Collectors.toList()));
  }

  public static PrimaryKeyMap firstN(int length) {
    return new PrimaryKeyMap(IntStream.range(0,length).mapToObj(Integer::valueOf).collect(Collectors.toList()));
  }

}
