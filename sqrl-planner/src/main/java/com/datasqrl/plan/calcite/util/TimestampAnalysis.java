/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.util;

import com.datasqrl.function.SqrlFunction;
import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.plan.calcite.table.TimestampHolder;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

import java.util.Optional;

public class TimestampAnalysis {

  public static Optional<TimestampHolder.Derived.Candidate> getPreservedTimestamp(
      @NonNull RexNode rexNode, @NonNull TimestampHolder.Derived timestamp) {
    if (!(CalciteUtil.isTimestamp(rexNode.getType()))) {
      return Optional.empty();
    }
    if (rexNode instanceof RexInputRef) {
      return timestamp.getOptCandidateByIndex(((RexInputRef) rexNode).getIndex());
    } else if (rexNode instanceof RexCall) {
      //Determine recursively but ensure there is only one timestamp
      RexCall call = (RexCall) rexNode;
      if (!isTimestampPreservingFunction(call)) {
        return Optional.empty();
      }
      //The above check guarantees that the call only has one timestamp operand which allows us to return the first (if any)
      return call.getOperands().stream().map(param -> getPreservedTimestamp(param, timestamp))
          .filter(Optional::isPresent).findFirst().orElse(Optional.empty());
    } else {
      return Optional.empty();
    }
  }

  private static boolean isTimestampPreservingFunction(RexCall call) {
    SqlOperator operator = call.getOperator();
    if (operator.getKind().equals(SqlKind.CAST)) {
      return true;
    }
    Optional<TimestampPreservingFunction> fnc = SqrlFunction.unwrapSqrlFunction(operator)
        .filter(op -> op instanceof TimestampPreservingFunction)
        .map(op -> (TimestampPreservingFunction) op)
        .filter(TimestampPreservingFunction::isTimestampPreserving);
    if (fnc.isPresent()) {
      //Internal validation that this is a legit timestamp-preserving function
      long numTimeParams = call.getOperands().stream().map(param -> param.getType())
          .filter(CalciteUtil::isTimestamp).count();
      Preconditions.checkArgument(numTimeParams == 1,
          "%s is an invalid time-preserving function as it allows %d number of timestamp arguments",
          operator, numTimeParams);
      return true;
    } else {
      return false;
    }
  }
}
