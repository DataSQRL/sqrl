package org.apache.calcite.sql;

import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import com.datasqrl.util.SqlNodePrinter;

@Getter
public class SqrlJoinQuery extends SqrlAssignment {

  private final SqlSelect query;

  public SqrlJoinQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlSelect query) {
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
    SqrlJoinQuery that = (SqrlJoinQuery) o;
    return Objects.equals(query, that.query) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(query, hints);
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    super.unparse(sqlWriter, i, i1);
    sqlWriter.print(SqlNodePrinter.printJoin(query));
  }

  @Override
  public <R, C> R accept(SqrlStatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
