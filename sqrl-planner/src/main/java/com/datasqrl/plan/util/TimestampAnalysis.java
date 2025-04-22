/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.plan.table.Timestamps;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.google.common.base.Preconditions;

import lombok.NonNull;
import lombok.Value;

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
      return false;
    }
    var inputRef = CalciteUtil.getNonAlteredInputRef(rexNode);
    if (inputRef.isPresent()) {
      return timestamp.isCandidate(inputRef.get());
    }
    if (!(rexNode instanceof RexCall call)) {
      return false;
    }
    var operator = call.getOperator();
    var operands = call.getOperands();
    if (operator.getKind().equals(SqlKind.CASE)) {
      Preconditions.checkArgument(operands.size()>=3 && operands.size()%2==1, "Invalid operand list: %s",operands );
      //Check that all operands are timestamps
      for (var i = 1; i < operands.size(); i=i+2) {
        if (!computesTimestamp(operands.get(i),timestamp)) {
			return false;
		}
      }
      return computesTimestamp(operands.get(operands.size()-1),timestamp); //check default/else
    } else if (call.getOperator().isName("greatest", false)) {
      return operands.stream().allMatch(rex -> computesTimestamp(rex,timestamp));
    }
    return false;
  }


  public static Optional<Timestamps.TimeWindow> extractTumbleWindow(int index, RexNode rexNode, RexBuilder rexBuilder, Timestamps timestamps) {
    if (!(rexNode instanceof RexCall call)) {
      return Optional.empty();
    }
    Optional<SqrlTimeTumbleFunction> fnc = FunctionUtil.getBridgedFunction(call.getOperator())
            .flatMap(FunctionUtil::getSqrlTimeTumbleFunction);
    if (fnc.isEmpty()) {
      return Optional.empty();
    }
    var tumbleFct = fnc.get();
    //Validate time bucketing function: First argument is timestamp, all others must be constants
    Preconditions.checkArgument(call.getOperands().size() > 0,
            "Tumble window function must have at least one argument");
    var timestamp = call.getOperands().get(0);
    var optIndex = CalciteUtil.getNonAlteredInputRef(timestamp);
    if (optIndex.isEmpty() || !timestamps.isCandidate(optIndex.get())) {
      return Optional.empty();
    }
    int timestampIdx = optIndex.get();
    List<RexNode> operands = new ArrayList<>();
    for (var i = 1; i < call.getOperands().size(); i++) {
      var operand = call.getOperands().get(i);
      Preconditions.checkArgument(RexUtil.isConstant(operand),
              "All non-timestamp arguments must be constants");
      operands.add(operand);
    }
    var operandValues = new long[0];
    if (!operands.isEmpty()) {
      var reducer = new ExpressionReducer();
      operandValues = reducer.reduce2Long(rexBuilder,
              operands); //throws exception if arguments cannot be reduced
    }
    var spec = tumbleFct.getSpecification(operandValues);
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
    for (var idx = 0; idx < aggregateCalls.size(); idx++) {
      var aggCall = aggregateCalls.get(idx);
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
