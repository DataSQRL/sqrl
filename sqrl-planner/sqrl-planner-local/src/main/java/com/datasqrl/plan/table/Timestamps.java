package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.plan.util.IndexMap;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

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
    Preconditions.checkArgument(type!=Type.UNDEFINED || indexes.isEmpty(), "Invalid timestamp definition");
    if (type==Type.AND && indexes.size()<2) type = Type.OR;
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
    return !indexes.isEmpty();
  }

  public Integer getBestCandidate(RelBuilder relB) {
    if (indexes.isEmpty()) {
      throw new UnsupportedOperationException("Undefined timestamp does not have candidates");
    } else if (is(Type.OR) || size()==1) { //Pick first
      return indexes.stream().sorted().findFirst().get();
    } else {  // is(Type.AND)
      SqrlRexUtil rexUtil = new SqrlRexUtil(relB);
      RelNode input = relB.peek();
      RexNode greatestTimestamp = rexUtil.greatestNotNull(asList(), input);
      rexUtil.appendColumn(relB, greatestTimestamp, ReservedName.SYSTEM_TIMESTAMP.getCanonical());
      return input.getRowType().getFieldCount();
    }
  }

  public Integer getAnyCandidate() {
    Preconditions.checkArgument(is(Type.OR));
    return indexes.stream().sorted().findFirst().get();
  }

  public boolean requiresInlining() {
    return is(Type.AND) && size()>1;
  }

  public Timestamps asBest(RelBuilder relB) {
    return Timestamps.ofFixed(getBestCandidate(relB));
  }

  public Integer getOnlyCandidate() {
    Preconditions.checkArgument(indexes.size()==1);
    return Iterables.getOnlyElement(indexes);
  }

  public boolean isCandidate(int index) {
    return indexes.contains(index);
  }

  public Predicate<Integer> isCandidatePredicate() {
    return indexes::contains;
  }

  public Iterable<Integer> getCandidates() {
    return indexes;
  }

  public List<Integer> asList() {
    return indexes.stream().sorted().collect(Collectors.toUnmodifiableList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Timestamps that = (Timestamps) o;
    return Objects.equals(indexes, that.indexes) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexes, type);
  }

  public static TimestampsBuilder build(Type type) {
    TimestampsBuilder builder = builder();
    builder.type(type);
    return builder;
  }


}
