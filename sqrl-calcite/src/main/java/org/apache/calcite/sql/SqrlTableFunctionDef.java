package org.apache.calcite.sql;

import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.List;

@Getter
public class SqrlTableFunctionDef extends SqrlSqlNode {

  private final List<SqrlTableParamDef> parameters;

  public SqrlTableFunctionDef(SqlParserPos location, List<SqrlTableParamDef> parameters) {
    super(location);
    this.parameters = parameters;
  }
}
