package org.apache.calcite.sql;

import java.util.List;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqrlJoinSetOperation extends SqrlJoinTerm {

  //Call + operands as jointerms
  public SqrlJoinSetOperation(SqlParserPos pos) {
    super(pos);
  }

  @Override
  public SqlOperator getOperator() {
    return null;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return null;
  }

  public <R, C> R accept(SqrlJoinTermVisitor<R, C> visitor, C context) {
    return visitor.visitJoinSetOperation(this, context);
  }
}
