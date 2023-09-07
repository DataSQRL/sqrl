package org.apache.calcite.sql;

import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlSqlQuery extends SqrlAssignment {

  protected final SqlNode query;

  public SqrlSqlQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlNode query) {
    super(location, hints, identifier, tableArgs);
    this.query = query;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqrlSqlQuery that = (SqrlSqlQuery) o;
    return Objects.equals(query, that.query) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, hints);
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    super.unparse(sqlWriter, i, i1);
  }

  @Override
  public <R, C> R accept(SqrlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
