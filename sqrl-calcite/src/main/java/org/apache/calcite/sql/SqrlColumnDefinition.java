package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import lombok.Getter;

@Getter
public class SqrlColumnDefinition extends SqrlSqlNode {

  private final SqlIdentifier columnName;
  private final SqlDataTypeSpec type;

  public SqrlColumnDefinition(SqlParserPos location, SqlIdentifier columnName, SqlDataTypeSpec type) {
    super(location);
    this.columnName = columnName;
    this.type = type;
  }
}
