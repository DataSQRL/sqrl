package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class ExpressionAssignment extends Assignment {

  private final SqlNode expression;
  private final SqlNodeList hints;

  public ExpressionAssignment(SqlParserPos location,
      NamePath name, SqlNode expression, SqlNodeList hints) {
    super(location, name);
    this.expression = expression;
    this.hints = hints;
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
}
