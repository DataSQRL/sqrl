package com.datasqrl.calcite;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;

public class ConvertletTable implements SqlRexConvertletTable {

  //workaround for FLINK-31279
  private static RexNode convertCall(SqlRexContext cx, SqlCall call) {
    SqlNode n = call.operand(0);
    SqlIntervalQualifier intervalQualifier = (SqlIntervalQualifier)call.operand(1);
    SqlIntervalLiteral literal = SqlLiteral.createInterval(1, "1", intervalQualifier, call.getParserPosition());
    SqlCall multiply = SqlStdOperatorTable.MULTIPLY
        .createCall(call.getParserPosition(), new SqlNode[]{literal, n});
    RexNode rexNode = cx.convertExpression(multiply);
    return rexNode;
  }

  @Override
  public SqlRexConvertlet get(SqlCall call) {
    if (call.getOperator() == SqlStdOperatorTable.INTERVAL) {
      return ConvertletTable::convertCall;
    }

    return StandardConvertletTable.INSTANCE.get(call);
  }
}
