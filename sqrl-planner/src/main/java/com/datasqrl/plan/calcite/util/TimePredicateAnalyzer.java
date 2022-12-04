package com.datasqrl.plan.calcite.util;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import com.google.common.collect.Iterables;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TimePredicateAnalyzer {

  public static final TimePredicateAnalyzer INSTANCE = new TimePredicateAnalyzer();

  public Optional<TimePredicate> extractTimePredicate(RexNode rexNode, RexBuilder rexBuilder,
      Predicate<Integer> isTimestampColumn) {
    if (!(rexNode instanceof RexCall)) {
      return Optional.empty();
    }
    RexCall call = (RexCall) rexNode;
    SqlKind opKind = call.getOperator().getKind();
    switch (opKind) {
      case GREATER_THAN:
      case LESS_THAN:
      case GREATER_THAN_OR_EQUAL:
      case LESS_THAN_OR_EQUAL:
      case EQUALS:
        break;
      default:
        return Optional.empty();
    }
    boolean strictlySmaller = opKind == SqlKind.LESS_THAN || opKind == SqlKind.GREATER_THAN;
    boolean flip = opKind == SqlKind.GREATER_THAN || opKind == SqlKind.GREATER_THAN_OR_EQUAL;
    boolean smaller = opKind != SqlKind.EQUALS;

    Pair<Set<Integer>, RexNode> processLeft = extractReferencesAndReplaceWithZero(
        call.getOperands().get(0), rexBuilder);
    Pair<Set<Integer>, RexNode> processRight = extractReferencesAndReplaceWithZero(
        call.getOperands().get(1), rexBuilder);

    //Can only reference a single column or timestamp function on each side
    if (processLeft.getKey().size() != 1 || processRight.getKey().size() != 1) {
      return Optional.empty();
    }
    //Check that the references are timestamp candidates or timestamp function
    int leftRef = Iterables.getOnlyElement(processLeft.getKey()),
        rightRef = Iterables.getOnlyElement(processRight.getKey());
    for (int ref : new int[]{leftRef, rightRef}) {
      if (ref > 0 && !isTimestampColumn.test(ref)) {
        return Optional.empty();
      }
    }
    ExpressionReducer reducer = new ExpressionReducer();
    long[] reduced;
    try {
      reduced = reducer.reduce2Long(rexBuilder,
          List.of(processLeft.getValue(), processRight.getValue()));
    } catch (IllegalArgumentException exception) {
      return Optional.empty();
    }
    assert reduced.length == 2;
    long interval_ms = reduced[1] - reduced[0];

    int smallerIndex = flip ? rightRef : leftRef;
    int largerIndex = flip ? leftRef : rightRef;
    if (flip) {
      interval_ms *= -1;
    }

    TimePredicate tp = new TimePredicate(smallerIndex, largerIndex,
        smaller ? (strictlySmaller ? SqlKind.LESS_THAN : SqlKind.LESS_THAN_OR_EQUAL)
            : SqlKind.EQUALS,
        interval_ms);
    return Optional.of(tp);
  }

  private Pair<Set<Integer>, RexNode> extractReferencesAndReplaceWithZero(RexNode rexNode,
      RexBuilder rexBuilder) {
    if (rexNode instanceof RexInputRef) {
      return Pair.of(Set.of(((RexInputRef) rexNode).getIndex()),
          rexBuilder.makeZeroLiteral(rexNode.getType()));
    }
    if (rexNode instanceof RexCall) {
      RexCall call = (RexCall) rexNode;
      if (SqrlFunction.unwrapSqrlFunction(call.getOperator())
          .filter(op -> op instanceof StdTimeLibraryImpl.NOW).isPresent()) {
        return Pair.of(Set.of(TimePredicate.NOW_INDEX),
            rexBuilder.makeZeroLiteral(rexNode.getType()));
      } else {
        //Map recursively
        final Set<Integer> allRefs = new HashSet<>();
        List<RexNode> newOps = call.getOperands().stream().map(op -> {
          Pair<Set<Integer>, RexNode> result = extractReferencesAndReplaceWithZero(op, rexBuilder);
          allRefs.addAll(result.getKey());
          return result.getValue();
        }).collect(Collectors.toList());
        return Pair.of(allRefs, rexBuilder.makeCall(call.getType(), call.getOperator(), newOps));
      }
    } else {
      return Pair.of(Set.of(), rexNode);
    }
  }


}
