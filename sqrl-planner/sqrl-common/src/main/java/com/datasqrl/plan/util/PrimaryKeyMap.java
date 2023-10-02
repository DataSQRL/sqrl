package com.datasqrl.plan.util;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
public class PrimaryKeyMap implements IndexMap, Serializable {

  @Singular
  final List<Integer> pkIndexes;

  public PrimaryKeyMap(List<Integer> pkIndexes) {
    Preconditions.checkArgument(Set.copyOf(pkIndexes).size() == pkIndexes.size(),
            "Duplicate primary key index: %s", pkIndexes);
    this.pkIndexes = pkIndexes;
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
    return new ArrayList<>(pkIndexes);
  }

  public int[] asArray() {
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
    return builder;
  }

  public static PrimaryKeyMap of(List<Integer> pks) {
    return new PrimaryKeyMap(List.copyOf(pks));
  }

  public static PrimaryKeyMap firstN(int length) {
    return new PrimaryKeyMap(IntStream.range(0,length).mapToObj(Integer::valueOf).collect(Collectors.toList()));
  }

}
