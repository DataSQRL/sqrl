package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlSqlQuery extends SqrlAssignment {

  protected final SqlNode query;

  public SqrlSqlQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlNode query) {
    super(location, hints, identifier, tableArgs);
    this.query = query;
  }
}
