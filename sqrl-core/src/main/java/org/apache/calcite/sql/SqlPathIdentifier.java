package org.apache.calcite.sql;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlPathIdentifier extends SqlIdentifier {

  @Getter
  @Setter
  private SqlPathJoin sqlPathJoin;

  public SqlPathIdentifier(List<String> names, SqlCollation collation,
      SqlParserPos pos,
      List<SqlParserPos> componentPositions) {
    super(names, collation, pos, componentPositions);
  }
}
