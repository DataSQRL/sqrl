package org.apache.calcite.sql;

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;

import lombok.Getter;

@Getter
public class SqrlTableFunctionDef extends SqrlSqlNode {

  private final List<SqrlTableParamDef> parameters;

  public SqrlTableFunctionDef(SqlParserPos location, List<SqrlTableParamDef> parameters) {
    super(location);
    this.parameters = parameters;
  }
}
