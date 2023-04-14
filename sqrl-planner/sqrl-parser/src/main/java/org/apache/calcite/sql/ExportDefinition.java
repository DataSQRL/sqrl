package org.apache.calcite.sql;

import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

@Getter
public class ExportDefinition extends SqrlStatement {

  protected final SqlIdentifier tablePath;
  protected final SqlIdentifier sinkPath;

  public ExportDefinition(SqlParserPos location, SqlIdentifier tablePath, NamePath namePath, SqlIdentifier sinkPath) {
    super(location, tablePath, namePath, Optional.empty());
    this.tablePath = tablePath;
    this.sinkPath = sinkPath;
  }

  @Override
  public SqlNode clone(SqlParserPos sqlParserPos) {
    return null;
  }

  @Override
  public void unparse(SqlWriter sqlWriter, int i, int i1) {
    sqlWriter.keyword("EXPORT");
    sinkPath.unparse(sqlWriter, i, i1);
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
