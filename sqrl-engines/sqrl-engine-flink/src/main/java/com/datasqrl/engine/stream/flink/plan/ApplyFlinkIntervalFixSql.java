package com.datasqrl.engine.stream.flink.plan;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;

public class ApplyFlinkIntervalFixSql extends SqlShuttle {

  @Override
  public SqlNode visit(SqlCall call) {
    //workaround for FLINK-31279
    if (call.getOperator() == SqlStdOperatorTable.MULTIPLY
        && call.getOperandList().get(0) instanceof SqlNumericLiteral
        && call.getOperandList().get(1) instanceof SqlIntervalLiteral) {
      return call.getOperator().createCall(call.getParserPosition(),
          call.getOperandList().get(1),
          call.getOperandList().get(0));
    }

    return super.visit(call);
  }
}