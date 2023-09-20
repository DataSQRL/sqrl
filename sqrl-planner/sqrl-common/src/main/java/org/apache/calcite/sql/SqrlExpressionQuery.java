package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlExpressionQuery extends SqrlAssignment {

  private final SqlNode expression;

  public SqrlExpressionQuery(SqlParserPos location, Optional<SqlNodeList> hints,
      SqlIdentifier identifier, Optional<SqrlTableFunctionDef> tableArgs, SqlNode expression) {
    super(location, hints, identifier, tableArgs);
    this.expression = expression;
  }
}
