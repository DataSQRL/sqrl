package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlFromQuery extends SqrlAssignment {

  private final SqlNode query;

  public SqrlFromQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlNode query) {
    super(location, hints, identifier, tableArgs);
    this.query = query;
  }
}
