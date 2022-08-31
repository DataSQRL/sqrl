package org.apache.calcite.sql;

import ai.datasqrl.parse.tree.name.NamePath;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class ExportDefinition extends SqrlStatement {

  protected final NamePath tablePath;
  protected final NamePath sinkPath;

  public ExportDefinition(SqlParserPos location, NamePath tablePath, NamePath sinkPath) {
    super(location);
    this.tablePath = tablePath;
    this.sinkPath = sinkPath;
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
