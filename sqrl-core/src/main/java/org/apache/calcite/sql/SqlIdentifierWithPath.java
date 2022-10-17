package org.apache.calcite.sql;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlIdentifierWithPath extends SqlIdentifier {

  @Getter
  private final SqlPathIdentifier table;

  public SqlIdentifierWithPath(SqlPathIdentifier table, List<String> names, SqlCollation collation, SqlParserPos pos,
      List<SqlParserPos> componentPositions) {
    super(names, collation, pos, componentPositions);
    this.table = table;
  }
}
