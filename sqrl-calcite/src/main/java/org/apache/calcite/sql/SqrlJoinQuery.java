package org.apache.calcite.sql;

import java.util.Optional;

import org.apache.calcite.sql.parser.SqlParserPos;

import lombok.Getter;

@Getter
public class SqrlJoinQuery extends SqrlAssignment {

  private final SqlSelect query;

  public SqrlJoinQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlSelect query) {
    super(location, hints, identifier, tableArgs);
    this.query = query;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
