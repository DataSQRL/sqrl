package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
@Setter
public abstract class SqrlStatement extends SqrlSqlNode {

  protected final SqlIdentifier identifier;
  protected final Optional<SqlNodeList> hints;

  protected SqrlStatement(
      SqlParserPos location, SqlIdentifier identifier, Optional<SqlNodeList> hints) {
    super(location);
    this.identifier = identifier;
    this.hints = hints;
  }

  public abstract <R, C> R accept(StatementVisitor<R, C> visitor, C context);
}
