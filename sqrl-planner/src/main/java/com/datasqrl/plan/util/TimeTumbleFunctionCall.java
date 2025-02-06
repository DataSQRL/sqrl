/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.FunctionUtil;
import com.google.common.base.Preconditions;

import lombok.Value;

@Value
public class TimeTumbleFunctionCall {

  private final SqrlTimeTumbleFunction operator;
  private final int timestampColumnIndex;
  private final long[] arguments;

  public SqrlTimeTumbleFunction.Specification getSpecification() {
    return operator.getSpecification(arguments);
  }

  public static Optional<TimeTumbleFunctionCall> from(RexNode rexNode, RexBuilder rexBuilder) {
    if (!(rexNode instanceof RexCall call)) {
        return Optional.empty();
    }
    Optional<SqrlTimeTumbleFunction> fnc = FunctionUtil.getBridgedFunction(call.getOperator())
        .flatMap(FunctionUtil::getSqrlTimeTumbleFunction);
    if (fnc.isEmpty()) {
        return Optional.empty();
    }
    var bucketFct = fnc.get();
    //Validate time bucketing function: First argument is timestamp, all others must be constants
    Preconditions.checkArgument(call.getOperands().size() > 0,
        "Time-bucketing function must have at least one argument");
    var timestamp = call.getOperands().getFirst();
    Preconditions.checkArgument(CalciteUtil.isTimestamp(timestamp.getType()),
        "Expected timestamp argument");
    Preconditions.checkArgument(timestamp instanceof RexInputRef);
    var timeColIndex = ((RexInputRef) timestamp).getIndex();
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
    return Optional.of(new TimeTumbleFunctionCall(bucketFct, timeColIndex, operandValues));
  }


}
