/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import com.datasqrl.function.TimestampPreservingFunction;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.plan.table.Timestamps;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.google.common.base.Preconditions;

import java.util.*;

import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class TimestampAnalysis {

  /**
   * Determines if the given RexNode computes a timestamp column.
   *
   * @param rexNode
   * @param timestamp
   * @return
   */
  public static boolean computesTimestamp(@NonNull RexNode rexNode, @NonNull Timestamps timestamp) {
    if (!(CalciteUtil.isTimestamp(rexNode.getType()))) {
//<<<<<<< HEAD
//      return Optional.empty();
//    }
//    if (rexNode instanceof RexInputRef) {
//      return timestamp.getOptCandidateByIndex(((RexInputRef) rexNode).getIndex());
//    } else if (rexNode instanceof RexCall) {
//      //Determine recursively but ensure there is only one timestamp
//      RexCall call = (RexCall) rexNode;
//      if (!isTimestampPreservingFunction(call)) {
//        return Optional.empty();
//      }
//      //The above check guarantees that the call only has one timestamp operand which allows us to return the first (if any)
//      return call.getOperands().stream().map(param -> getPreservedTimestamp(param, timestamp))
//          .filter(Optional::isPresent).findFirst().orElse(Optional.empty());
//    } else {
//      return Optional.empty();
//    }
//  }
//
//  private static boolean isTimestampPreservingFunction(RexCall call) {
//    SqlOperator operator = call.getOperator();
//    if (operator.getKind().equals(SqlKind.CAST)) {
//      return true;
//    }
//    Optional<TimestampPreservingFunction> fnc = Optional.of(operator)
//        .flatMap(f-> FunctionUtil.getSqrlFunction(operator))
//        .flatMap(FunctionUtil::getTimestampPreservingFunction)
//        .filter(TimestampPreservingFunction::isTimestampPreserving);
//    if (fnc.isPresent()) {
//      //Internal validation that this is a legit timestamp-preserving function
//      long numTimeParams = call.getOperands().stream().map(param -> param.getType())
//          .filter(CalciteUtil::isTimestamp).count();
//      Preconditions.checkArgument(numTimeParams == 1,
//          "%s is an invalid time-preserving function as it allows %s number of timestamp arguments",
//          operator, numTimeParams);
//      return true;
//    } else {
//=======
//>>>>>>> 80b9c0e31 (0.5 refactor)
      return false;
    }
    Optional<Integer> inputRef = CalciteUtil.getNonAlteredInputRef(rexNode);
    if (inputRef.isPresent()) {
      return timestamp.isCandidate(inputRef.get());
    }
    if (!(rexNode instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) rexNode;
    SqlOperator operator = call.getOperator();
    List<RexNode> operands = call.getOperands();
    if (operator.getKind().equals(SqlKind.CASE)) {
      Preconditions.checkArgument(operands.size()>=3 && operands.size()%2==1, "Invalid operand list: %s",operands );
      //Check that all operands are timestamps
      for (int i = 1; i < operands.size(); i=i+2) {
        if (!computesTimestamp(operands.get(i),timestamp)) return false;
      }
      return computesTimestamp(operands.get(operands.size()-1),timestamp); //check default/else
    } else if (call.getOperator().isName("greatest", false)) {
      return operands.stream().allMatch(rex -> computesTimestamp(rex,timestamp));
    }
    return false;
  }


  public static Optional<Timestamps.TimeWindow> extractTumbleWindow(int index, RexNode rexNode, RexBuilder rexBuilder, Timestamps timestamps) {
    if (!(rexNode instanceof RexCall)) {
      return Optional.empty();
    }
    RexCall call = (RexCall) rexNode;
    Optional<SqrlTimeTumbleFunction> fnc = FunctionUtil.getSqrlFunction(call.getOperator())
            .filter(o -> o instanceof SqrlTimeTumbleFunction)
            .map(o -> (SqrlTimeTumbleFunction) o);
    if (fnc.isEmpty()) {
      return Optional.empty();
    }
    SqrlTimeTumbleFunction tumbleFct = fnc.get();
    //Validate time bucketing function: First argument is timestamp, all others must be constants
    Preconditions.checkArgument(call.getOperands().size() > 0,
            "Tumble window function must have at least one argument");
    RexNode timestamp = call.getOperands().get(0);
    Optional<Integer> optIndex = CalciteUtil.getNonAlteredInputRef(timestamp);
    if (optIndex.isEmpty() || !timestamps.isCandidate(optIndex.get())) {
      return Optional.empty();
    }
    int timestampIdx = optIndex.get();
    List<RexNode> operands = new ArrayList<>();
    for (int i = 1; i < call.getOperands().size(); i++) {
      RexNode operand = call.getOperands().get(i);
      Preconditions.checkArgument(RexUtil.isConstant(operand),
              "All non-timestamp arguments must be constants");
      operands.add(operand);
    }
    long[] operandValues = new long[0];
    if (!operands.isEmpty()) {
      ExpressionReducer reducer = new ExpressionReducer();
      operandValues = reducer.reduce2Long(rexBuilder,
              operands); //throws exception if arguments cannot be reduced
    }
    SqrlTimeTumbleFunction.Specification spec = tumbleFct.getSpecification(operandValues);
    return Optional.of(new Timestamps.SimpleTumbleWindow(index, timestampIdx, spec.getWindowWidthMillis(),
            spec.getWindowOffsetMillis()));
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
