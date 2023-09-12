package org.apache.calcite.sql;

import com.datasqrl.util.SqlNodePrinter;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Objects;
import java.util.Optional;

@Getter
public class SqrlFromQuery extends SqrlAssignment {

  private final SqlNode query;

  public SqrlFromQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
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
    SqrlFromQuery that = (SqrlFromQuery) o;
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
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
