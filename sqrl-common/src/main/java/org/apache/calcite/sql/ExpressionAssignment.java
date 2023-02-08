package org.apache.calcite.sql;

import com.datasqrl.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class ExpressionAssignment extends Assignment {

  private final Optional<List<TableFunctionArgument>> tableArgs;
  private final SqlNode expression;

  public ExpressionAssignment(SqlParserPos location,
      SqlIdentifier identifier, NamePath namePath, Optional<List<TableFunctionArgument>> tableArgs, SqlNode expression,
      Optional<SqlNodeList> hints) {
    super(location, identifier, namePath, hints);
    this.tableArgs = tableArgs;
    this.expression = expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExpressionAssignment that = (ExpressionAssignment) o;
    return Objects.equals(expression, that.expression) && Objects.equals(hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expression, hints);
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {

  }

  @Override
  public void validate(SqlValidator sqlValidator, SqlValidatorScope sqlValidatorScope) {

  }

  @Override
  public <R> R accept(SqlVisitor<R> sqlVisitor) {
    return null;
  }

  @Override
  public boolean equalsDeep(SqlNode sqlNode, Litmus litmus) {
    return false;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
