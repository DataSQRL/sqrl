package org.apache.calcite.sql;

import java.util.Optional;

import org.apache.calcite.sql.parser.SqlParserPos;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class SqrlAssignment extends SqrlStatement {

  private Optional<SqrlTableFunctionDef> tableArgs;

  protected SqrlAssignment(SqlParserPos location, Optional<SqlNodeList> hints,
      SqlIdentifier identifier, Optional<SqrlTableFunctionDef> tableArgs) {
    super(location, identifier, hints);
    this.tableArgs = tableArgs;
  }
}
