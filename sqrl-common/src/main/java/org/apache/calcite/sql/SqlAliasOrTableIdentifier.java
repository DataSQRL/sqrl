package org.apache.calcite.sql;

import java.util.List;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAliasOrTableIdentifier extends SqlIdentifier {

  public SqlAliasOrTableIdentifier(List<String> names, SqlCollation collation,
      SqlParserPos pos,
      List<SqlParserPos> componentPositions) {
    super(names, collation, pos, componentPositions);
  }

}
