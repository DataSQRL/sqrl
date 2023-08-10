package org.apache.calcite.sql;

import com.datasqrl.canonicalizer.NamePath;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public abstract class Assignment extends SqrlStatement {
  private final Optional<List<TableFunctionArgument>> tableArgs;

  protected Assignment(SqlParserPos location, SqlIdentifier identifier, NamePath namePath, Optional<SqlNodeList> hints, Optional<List<TableFunctionArgument>> tableArgs) {
    super(location, identifier, namePath, hints);
    this.tableArgs = tableArgs;
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    this.identifier.unparse(sqlWriter, i, i1);
    sqlWriter.keyword(":=");
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
