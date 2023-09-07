package org.apache.calcite.sql;

import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Optional;

@Getter
public class SqrlTableParamDef extends SqlNode {

  private final SqlIdentifier name;
  private final SqlDataTypeSpec type;
  private final Optional<SqlNode> defaultValue;
  private final int index;
  private final boolean isInternal;

  public SqrlTableParamDef(SqlParserPos location, SqlIdentifier name, SqlDataTypeSpec type,
      Optional<SqlNode> literal, int index, boolean isInternal) {
    super(location);
    this.name = name;
    this.type = type;
    this.defaultValue = literal;
    this.index = index;
    this.isInternal = isInternal;
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
