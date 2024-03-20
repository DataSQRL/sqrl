package org.apache.calcite.sql;

import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqrlDistinctQuery extends SqrlAssignment {

  private final SqlSelect select;

  public SqrlDistinctQuery(SqlParserPos location, Optional<SqlNodeList> hints,
      SqlIdentifier identifier, Optional<SqrlTableFunctionDef> tableArgs, SqlSelect select) {
    super(location, hints, identifier, tableArgs);
    this.select = select;
  }
}
