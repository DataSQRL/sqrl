/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.function.StdTimeLibraryImpl;
import com.datasqrl.plan.table.TimestampInference;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class TimestampAnalysis {

  public static Optional<TimestampInference.Candidate> getPreservedTimestamp(
      @NonNull RexNode rexNode, @NonNull TimestampInference timestamp) {
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
    Optional<TimestampPreservingFunction> fnc = Optional.of(operator)
        .flatMap(f-> SqrlRexUtil.getSqrlFunction(operator))
        .filter(op -> op instanceof TimestampPreservingFunction)
        .map(op -> (TimestampPreservingFunction) op)
        .filter(TimestampPreservingFunction::isTimestampPreserving);
    if (fnc.isPresent()) {
      //Internal validation that this is a legit timestamp-preserving function
      long numTimeParams = call.getOperands().stream().map(param -> param.getType())
          .filter(CalciteUtil::isTimestamp).count();
      Preconditions.checkArgument(numTimeParams == 1,
          "%s is an invalid time-preserving function as it allows %s number of timestamp arguments",
          operator, numTimeParams);
      return true;
    } else {
      return false;
    }
  }


  @Value
  public static class MaxTimestamp {
    TimestampInference.Candidate candidate;
    int timestampIdx;
    int aggCallIdx;
  }

  public static Optional<MaxTimestamp> findMaxTimestamp(List<AggregateCall> aggregateCalls,
                                                        TimestampInference timestamp) {
    for (int idx = 0; idx < aggregateCalls.size(); idx++) {
      AggregateCall aggCall = aggregateCalls.get(idx);
      if (aggCall.getAggregation().equals(SqlStdOperatorTable.MAX)
          && aggCall.getArgList().size()==1 && aggCall.filterArg==-1
          && !aggCall.isApproximate() && !aggCall.isDistinct()
          && aggCall.collation.getFieldCollations().isEmpty()
      ) {
        //Check if input is a timestamp candidate
        int inputIdx = aggCall.getArgList().get(0);
        Optional<TimestampInference.Candidate> candidate = timestamp.getOptCandidateByIndex(inputIdx);
        if (candidate.isPresent()) {
          return Optional.of(new MaxTimestamp(candidate.get(), inputIdx, idx));
        }
      }
    }
    return Optional.empty();
  }

}
