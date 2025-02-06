package org.apache.calcite.sql;

import java.util.Optional;

import org.apache.calcite.sql.parser.SqlParserPos;

import lombok.Getter;

@Getter
public class SqrlFromQuery extends SqrlAssignment {

  private final SqlNode query;

  public SqrlFromQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlNode query) {
    super(location, hints, identifier, tableArgs);
    this.query = query;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
