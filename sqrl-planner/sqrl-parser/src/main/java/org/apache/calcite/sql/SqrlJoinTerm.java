package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

public abstract class SqrlJoinTerm extends SqlCall {

  SqrlJoinTerm(SqlParserPos pos) {
    super(pos);
  }

  public abstract <R, C> R accept(SqrlJoinTermVisitor<R, C> visitor, C context);

  public interface SqrlJoinTermVisitor<R, C> {

    R visitJoinPath(SqrlJoinPath sqrlJoinPath, C context);

    R visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation, C context);
  }
}
