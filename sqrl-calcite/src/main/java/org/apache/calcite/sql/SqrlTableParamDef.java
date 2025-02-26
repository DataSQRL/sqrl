package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlTableParamDef extends SqrlSqlNode {

  private final SqlIdentifier name;
  private final SqlDataTypeSpec type;
  private final Optional<SqlNode> defaultValue;
  private final int index;

  public SqrlTableParamDef(
      SqlParserPos location,
      SqlIdentifier name,
      SqlDataTypeSpec type,
      Optional<SqlNode> literal,
      int index) {
    super(location);
    this.name = name;
    this.type = type;
    this.defaultValue = literal;
    this.index = index;
  }
}
