package com.datasqrl.plan.table;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
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
  Set<Integer> candidates;
  Type type;
  @Singular
  Set<TimeWindow> windows;


  public static Timestamps UNDEFINED = new Timestamps(ImmutableSet.of(), Type.UNDEFINED);

  public Timestamps(Set<Integer> candidates, Type type) {
    this(candidates,type, Set.of());
  }

  public Timestamps(Set<Integer> candidates, Type type, Set<TimeWindow> windows) {
    Preconditions.checkArgument(type!=Type.UNDEFINED || (candidates.isEmpty() && windows.isEmpty()), "Invalid timestamp definition");
    Preconditions.checkArgument(windows.stream().map(TimeWindow::getTimestampIndex).allMatch(candidates::contains));
    if (type==Type.AND && candidates.size()<2) type = Type.OR;
    this.candidates = candidates;
    this.type = type;
    this.windows = windows;
  }

  public boolean is(Type type) {
    return this.type==type;
  }

  public static Timestamps ofFixed(int index) {
    return new Timestamps(ImmutableSet.of(index), Type.OR);
  }

  public int size() {
    return candidates.size();
  }

  public boolean hasCandidates() {
    return !candidates.isEmpty();
  }

  public Integer getBestCandidate(RelBuilder relB) {
    if (candidates.isEmpty()) {
      throw new UnsupportedOperationException("Undefined timestamp does not have candidates");
    } else if (is(Type.OR) || size()==1) { //Pick first
      return candidates.stream().sorted().findFirst().get();
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
    return candidates.stream().sorted().findFirst().get();
  }

  public boolean requiresInlining() {
    return is(Type.AND) && size()>1;
  }

  public Timestamps asBest(RelBuilder relB) {
    return Timestamps.ofFixed(getBestCandidate(relB));
  }

  public Integer getOnlyCandidate() {
    Preconditions.checkArgument(candidates.size()==1);
    return Iterables.getOnlyElement(candidates);
  }

  public boolean isCandidate(int index) {
    return candidates.contains(index);
  }

  public Predicate<Integer> isCandidatePredicate() {
    return candidates::contains;
  }

  public Collection<Integer> getCandidates() {
    return candidates;
  }

  public List<Integer> asList() {
    return candidates.stream().sorted().collect(Collectors.toUnmodifiableList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Timestamps that = (Timestamps) o;
    return Objects.equals(candidates, that.candidates) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(candidates, type);
  }

  public static TimestampsBuilder build(Type type) {
    TimestampsBuilder builder = builder();
    builder.type(type);
    return builder;
  }

  public interface TimeWindow {

    int getTimestampIndex();

    TimeWindow withTimestampIndex(int newIndex);

    boolean qualifiesWindow(List<Integer> indexes);

  }

  /**
   * A time window created by a {@link SqrlTimeTumbleFunction}.
   */
  @Value
  public static class SimpleTumbleWindow implements TimeWindow {

    int windowIndex;
    int timestampIndex;
    long windowWidthMillis;
    long windowOffsetMillis;

    @Override
    public TimeWindow withTimestampIndex(int newIndex) {
      return new SimpleTumbleWindow(this.windowIndex, newIndex, this.windowWidthMillis, this.windowOffsetMillis);
    }

    @Override
    public boolean qualifiesWindow(List<Integer> indexes) {
      return indexes.contains(windowIndex);
    }
  }

  /**
   * A time-window created by one of Flink's TVF window functions
   */
  @Value
  public static class FlinkTVFWindow implements TimeWindow {

    int windowStartIndex;
    int windowEndIndex;
    int windowTimeIndex;

    @Override
    public int getTimestampIndex() {
      return windowTimeIndex;
    }

    @Override
    public TimeWindow withTimestampIndex(int newIndex) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean qualifiesWindow(List<Integer> indexes) {
      return indexes.contains(windowStartIndex) && indexes.contains(windowEndIndex);
    }
  }


}
