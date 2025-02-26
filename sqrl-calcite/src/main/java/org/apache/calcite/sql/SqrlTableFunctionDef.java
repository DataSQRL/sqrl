package org.apache.calcite.sql;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlTableFunctionDef extends SqrlSqlNode {

  private final List<SqrlTableParamDef> parameters;

  public SqrlTableFunctionDef(SqlParserPos location, List<SqrlTableParamDef> parameters) {
    super(location);
    this.parameters = parameters;
  }
}
