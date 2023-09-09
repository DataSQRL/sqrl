package com.datasqrl.calcite.schema.sql;

import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class SqlJoinBuilder {

  private final SqlJoin join;

  public SqlJoinBuilder(SqlJoin call) {
    this.join = new SqlJoin(SqlParserPos.ZERO,
        call.operand(0),
        call.operand(1),
        call.operand(2),
        call.operand(3),
        call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO))
            ? JoinConditionType.ON.symbol(SqlParserPos.ZERO)
            : call.operand(4),
        call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO))
            ? SqlLiteral.createBoolean(true, SqlParserPos.ZERO)
            : call.operand(5));
  }

  public SqlJoinBuilder setLeft(SqlNode sqlNode) {
    join.setLeft(sqlNode);
    return this;
  }

  public SqlJoinBuilder setRight(SqlNode sqlNode) {
    join.setRight(sqlNode);
    return this;
  }

  public SqlJoinBuilder lateral() {
    this.setRight(SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, join.getRight()));
    return this;
  }

  public SqlNode build() {
    return join;
  }

  public SqlJoinBuilder rewriteExpressions(SqlShuttle shuttle) {
    join.setOperand(5, join.getCondition().accept(shuttle));
    return this;
  }
}
