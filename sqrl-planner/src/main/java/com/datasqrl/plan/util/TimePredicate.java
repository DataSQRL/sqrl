/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import lombok.Value;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Represents a timestamp predicate that is normalized into the form: smallerIndex <=/= largerIndex
 * + interval The index values refer to timestamp columns in a relation by index or a special
 * timestamp designator.
 */
@Value
public class TimePredicate {

  public static final int NOW_INDEX = -10;

  final int smallerIndex;
  final int largerIndex;
  final SqlKind comparison; //equals, less_than, or less_than_equals
  final long intervalLength; //in milliseconds

  public List<Integer> getIndexes() {
    return List.of(smallerIndex, largerIndex);
  }

  public boolean hasTimestampFunction() {
    return smallerIndex < 0 || largerIndex < 0;
  }

  public boolean isNowPredicate() {
    return smallerIndex == NOW_INDEX && intervalLength > 0;
  }

  public boolean isUpperBound() {
    return intervalLength <= 0;
  }

  public boolean isEquality() {
    return comparison == SqlKind.EQUALS;
  }

  public long normalizedIntervalLength() {
      if (comparison == SqlKind.LESS_THAN) {
          return intervalLength - 1;
      } else {
          return intervalLength;
      }
  }

  public Optional<TimePredicate> and(TimePredicate other) {
    Preconditions.checkArgument(
        smallerIndex == other.smallerIndex && largerIndex == other.largerIndex,
        "Incompatible indexes: [%s] vs [%s]", this, other);
    if (isEquality() && other.isEquality()) {
        if (intervalLength == other.intervalLength) {
            return Optional.of(this);
        } else {
            return Optional.empty();
        }
    } else if (isEquality()) {
        if (intervalLength <= other.normalizedIntervalLength()) {
            return Optional.of(this);
        } else {
            return Optional.empty();
        }
    } else if (other.isEquality()) {
        if (other.intervalLength <= normalizedIntervalLength()) {
            return Optional.of(other);
        } else {
            return Optional.empty();
        }
    } else { //both are inequalities
      SqlKind comp;
      long intLength;
      if (comparison == other.comparison) {
        comp = comparison;
        intLength = Long.min(intervalLength, other.intervalLength);
      } else {
        comp = SqlKind.LESS_THAN_OR_EQUAL;
        intLength = Long.min(normalizedIntervalLength(), other.normalizedIntervalLength());
      }
      return Optional.of(new TimePredicate(smallerIndex, largerIndex, comp, intLength));
    }
  }

  public TimePredicate inverseWithInterval(long interval_ms) {
    return new TimePredicate(largerIndex, smallerIndex, comparison, interval_ms);
  }

  public TimePredicate remap(IndexMap map) {
    return new TimePredicate(smallerIndex < 0 ? smallerIndex : map.map(smallerIndex),
        largerIndex < 0 ? largerIndex : map.map(largerIndex), comparison, intervalLength);
  }

  public RexNode createRexNode(RexBuilder rexBuilder, Function<Integer, RexInputRef> createInputRef,
      boolean useCurrentTime) {
    RexNode smallerRef = createRef(smallerIndex, rexBuilder, createInputRef, useCurrentTime);
    RexNode largerRef = createRef(largerIndex, rexBuilder, createInputRef, useCurrentTime);
    SqlOperator op;
    switch (comparison) {
      case EQUALS:
        op = SqlStdOperatorTable.EQUALS;
        break;
      case LESS_THAN:
        op = SqlStdOperatorTable.LESS_THAN;
        break;
      case LESS_THAN_OR_EQUAL:
        op = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        break;
      default:
        throw new UnsupportedOperationException(comparison.name());
    }

    if (intervalLength < 0) {
      smallerRef = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, smallerRef,
          CalciteUtil.makeTimeInterval(intervalLength, rexBuilder));
    } else if (intervalLength > 0) {
      largerRef = rexBuilder.makeCall(SqlStdOperatorTable.DATETIME_PLUS, largerRef,
          CalciteUtil.makeTimeInterval(intervalLength, rexBuilder));
    }
    return rexBuilder.makeCall(op, smallerRef, largerRef);
  }

  private static RexNode createRef(int index, RexBuilder rexBuilder,
      Function<Integer, RexInputRef> createInputRef, boolean useCurrentTime) {
      if (index >= 0) {
          return createInputRef.apply(index);
      } else if (index == NOW_INDEX) {
          if (useCurrentTime) {
              return rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP);
          } else {
              return rexBuilder.makeCall(lightweightOp("now", ReturnTypes.explicit(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3)));
          }
      }
    throw new UnsupportedOperationException("Invalid index: " + index);
  }


}
