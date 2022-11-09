package org.apache.calcite.sql;

import java.util.List;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

public class SqrlJoinSetOperation extends SqrlJoinTerm {

  //Call + operands as jointerms
  public SqrlJoinSetOperation(SqlParserPos pos) {
    super(pos);
  }

  @NotNull
  @Override
  public SqlOperator getOperator() {
    return null;
  }

  @NotNull
  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  public <R, C> R accept(SqrlJoinTermVisitor<R, C> visitor, C context) {
    return visitor.visitJoinSetOperation(this, context);
  }
}
