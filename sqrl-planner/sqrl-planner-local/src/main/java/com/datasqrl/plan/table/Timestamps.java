package com.datasqrl.plan.table;

import com.datasqrl.plan.util.IndexMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class Timestamps {

  public enum Type {
    AND, OR, UNDEFINED;
  }

  @Singular
  Set<Integer> indexes;
  Type type;

  public static Timestamps UNDEFINED = new Timestamps(ImmutableSet.of(), Type.UNDEFINED);

  public Timestamps(Set<Integer> indexes, Type type) {
    Preconditions.checkArgument((type==Type.UNDEFINED && indexes.isEmpty()) ||
        (type!=Type.UNDEFINED && !indexes.isEmpty()), "Invalid timestamp definition");
    this.indexes = indexes;
    this.type = type;
  }

  public boolean is(Type type) {
    return this.type==type;
  }

  public static Timestamps ofFixed(int index) {
    return new Timestamps(ImmutableSet.of(index), Type.OR);
  }

  public int size() {
    return indexes.size();
  }

  public boolean hasCandidates() {
    Preconditions.checkArgument(is(Type.OR));
    return !indexes.isEmpty();
  }

  public Integer getBestCandidate() {
    Preconditions.checkArgument(hasCandidates());
    return indexes.stream().sorted().findFirst().get();
  }

  public Timestamps asBest() {
    return Timestamps.ofFixed(getBestCandidate());
  }

  public Integer getOnlyCandidate() {
    Preconditions.checkArgument(indexes.size()==1);
    return Iterables.getOnlyElement(indexes);
  }

  public boolean isCandidate(int index) {
    Preconditions.checkArgument(is(Type.OR));
    return indexes.contains(index);
  }

  public Predicate<Integer> isCandidatePredicate() {
    Preconditions.checkArgument(is(Type.OR));
    return indexes::contains;
  }

  public Iterable<Integer> getCandidates() {
    return indexes;
  }

  public List<Integer> asList() {
    return indexes.stream().sorted().collect(Collectors.toUnmodifiableList());
  }

  public Set<Integer> asSet() {
    return indexes;
  }

  public Timestamps remapIndexes(IndexMap map) {
    return new Timestamps(indexes.stream().map(map::map).collect(Collectors.toUnmodifiableSet()), type);
  }

  public static TimestampsBuilder build(Type type) {
    TimestampsBuilder builder = builder();
    builder.type(type);
    return builder;
  }


}
