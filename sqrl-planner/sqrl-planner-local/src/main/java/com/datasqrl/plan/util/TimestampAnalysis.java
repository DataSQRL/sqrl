/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.function.StdTimeLibraryImpl;
import com.datasqrl.plan.table.TimestampInference;
import com.datasqrl.plan.table.Timestamps;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.SqrlRexUtil;
import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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

  /**
   * Returns an optional with the input index of the preserved timestamp or -1 if multiple timestamps
   * are preserved together (e.g. in a greatest function call). Returns empty if no timestamp is preserved.
   *
   * @param rexNode
   * @param timestamp
   * @return
   */
  public static Optional<Integer> getPreservedTimestamp(
      @NonNull RexNode rexNode, @NonNull Timestamps timestamp) {
    if (!(CalciteUtil.isTimestamp(rexNode.getType()))) {
      return Optional.empty();
    }
    if (rexNode instanceof RexInputRef) {
      int index = ((RexInputRef) rexNode).getIndex();
      return timestamp.isCandidate(index)?Optional.of(index):Optional.empty();
    } else if (rexNode instanceof RexCall) {
      //Determine recursively but ensure there is only one timestamp
      RexCall call = (RexCall) rexNode;
      return isTimestampPreservingFunction(call, timestamp);
    } else {
      return Optional.empty();
    }
  }

  private static Optional<Integer> isTimestampPreservingFunction(RexCall call, Timestamps timestamp) {
    SqlOperator operator = call.getOperator();
    if (operator.getKind().equals(SqlKind.CAST)) {
      return getPreservedTimestamp(call.getOperands().get(0), timestamp);
    } else if (call.getOperator().isName("greatest", false)) {
      if (call.getOperands().stream().map(n -> getPreservedTimestamp(n, timestamp)).allMatch(Optional::isPresent)) {
        return Optional.of(-1);
      } else {
        return Optional.empty();
      }
    }
    Optional<TimestampPreservingFunction> fnc = Optional.of(operator)
        .flatMap(f-> FunctionUtil.getSqrlFunction(operator))
        .filter(op -> op instanceof TimestampPreservingFunction)
        .map(op -> (TimestampPreservingFunction) op)
        .filter(TimestampPreservingFunction::isTimestampPreserving);
    if (fnc.isPresent()) {
      //Internal validation that this is a legit timestamp-preserving function
      Optional<Optional<Integer>> foundTimestamp = call.getOperands().stream().map(node -> getPreservedTimestamp(node, timestamp))
          .filter(Optional::isPresent).findFirst();
      if (foundTimestamp.isPresent()) {
        if (fnc.get().preservesSingleTimestampArgument()) {
          return foundTimestamp.get();
        } else {
          return Optional.of(-1); //Mapping multiple timestamps so there isn't "one" to return.
        }
      }
    }
    return Optional.empty();
  }


  @Value
  public static class MaxTimestamp {
    int timestampIdx;
    int aggCallIdx;
  }

  public static Optional<MaxTimestamp> findMaxTimestamp(List<AggregateCall> aggregateCalls,
                                                        Timestamps timestamp) {
    for (int idx = 0; idx < aggregateCalls.size(); idx++) {
      AggregateCall aggCall = aggregateCalls.get(idx);
      if (aggCall.getAggregation().equals(SqlStdOperatorTable.MAX)
          && aggCall.getArgList().size()==1 && aggCall.filterArg==-1
          && !aggCall.isApproximate() && !aggCall.isDistinct()
          && aggCall.collation.getFieldCollations().isEmpty()
      ) {
        //Check if input is a timestamp candidate
        int inputIdx = aggCall.getArgList().get(0);
        if (timestamp.isCandidate(inputIdx)) {
          return Optional.of(new MaxTimestamp(inputIdx, idx));
        }
      }
    }
    return Optional.empty();
  }

}
