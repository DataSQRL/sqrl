package org.apache.calcite.sql;

import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlJoinQuery extends SqrlAssignment {

  private final SqlSelect query;

  public SqrlJoinQuery(SqlParserPos location, Optional<SqlNodeList> hints, SqlIdentifier identifier,
      Optional<SqrlTableFunctionDef> tableArgs, SqlSelect query) {
    super(location, hints, identifier, tableArgs);
    this.query = query;
  }
}
