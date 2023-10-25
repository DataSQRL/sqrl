package com.datasqrl.engine.stream.flink.plan;

import lombok.AllArgsConstructor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;

@AllArgsConstructor
public class ApplyFlinkIntervalFixRex extends RexShuttle {
  RexBuilder rexBuilder;

  @Override
  public RexNode visitCall(RexCall call) {
    //workaround for FLINK-31279
    if (call.getOperator() == SqlStdOperatorTable.MULTIPLY
        && call.getOperands().get(1).getType() instanceof IntervalSqlType
    ) {
      return rexBuilder.makeCall(call.getOperator(),
          call.getOperands().get(1),
          call.getOperands().get(0)
      );
    }

    return super.visitCall(call);
  }
}
