package org.apache.calcite.sql;

import java.util.List;
import java.util.Optional;

import org.apache.calcite.sql.parser.SqlParserPos;

import lombok.Getter;

@Getter
public class SqrlCreateDefinition extends SqrlStatement {

  protected final SqlIdentifier name;
  protected final List<SqrlColumnDefinition> columns;

  public SqrlCreateDefinition(SqlParserPos location, SqlIdentifier name,
      List<SqrlColumnDefinition> columns) {
    super(location, name, Optional.empty());
    this.name = name;
    this.columns = columns;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
