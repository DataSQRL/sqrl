/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.tuple.Pair;

import com.datasqrl.calcite.SqrlRexUtil;
import com.google.common.collect.Iterables;

public class TimePredicateAnalyzer {

  public static final TimePredicateAnalyzer INSTANCE = new TimePredicateAnalyzer();

  public Optional<TimePredicate> extractTimePredicate(RexNode rexNode, RexBuilder rexBuilder,
      Predicate<Integer> isTimestampColumn) {
    if (!(rexNode instanceof RexCall call)) {
      return Optional.empty();
    }
    var opKind = call.getOperator().getKind();
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
    var strictlySmaller = opKind == SqlKind.LESS_THAN || opKind == SqlKind.GREATER_THAN;
    var flip = opKind == SqlKind.GREATER_THAN || opKind == SqlKind.GREATER_THAN_OR_EQUAL;
    var smaller = opKind != SqlKind.EQUALS;

    var processLeft = extractReferencesAndReplaceWithZero(
        call.getOperands().get(0), rexBuilder);
    var processRight = extractReferencesAndReplaceWithZero(
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
    var reducer = new ExpressionReducer();
    long[] reduced;
    try {
      reduced = reducer.reduce2Long(rexBuilder,
          List.of(processLeft.getValue(), processRight.getValue()));
    } catch (IllegalArgumentException exception) {
      return Optional.empty();
    }
    assert reduced.length == 2;
    var interval_ms = reduced[1] - reduced[0];

    var smallerIndex = flip ? rightRef : leftRef;
    var largerIndex = flip ? leftRef : rightRef;
    if (flip) {
      interval_ms *= -1;
    }

    var tp = new TimePredicate(smallerIndex, largerIndex,
        smaller ? (strictlySmaller ? SqlKind.LESS_THAN : SqlKind.LESS_THAN_OR_EQUAL)
            : SqlKind.EQUALS,
        interval_ms);
    return Optional.of(tp);
  }

  private Pair<Set<Integer>, RexNode> extractReferencesAndReplaceWithZero(RexNode rexNode,
      RexBuilder rexBuilder) {
    if (rexNode instanceof RexInputRef ref) {
      return Pair.of(Set.of(ref.getIndex()),
          rexBuilder.makeZeroLiteral(rexNode.getType()));
    }
    if (rexNode instanceof RexCall call) {
      if (SqrlRexUtil.isNOW(call.getOperator())) {
        return Pair.of(Set.of(TimePredicate.NOW_INDEX),
            rexBuilder.makeZeroLiteral(rexNode.getType()));
      } else {
        //Map recursively
        final Set<Integer> allRefs = new HashSet<>();
        List<RexNode> newOps = call.getOperands().stream().map(op -> {
          var result = extractReferencesAndReplaceWithZero(op, rexBuilder);
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
