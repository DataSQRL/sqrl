package org.apache.calcite.sql;

import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqlPathJoin extends SqlJoin {

  public SqlPathJoin(List<String> names, SqlParserPos pos, List<SqlParserPos> componentPositions) {
    super(pos, createLeft(names, pos, componentPositions), SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.IMPLICIT.symbol(pos), createRight(names, pos, componentPositions),
        JoinConditionType.NONE.symbol(pos), null);
//    SqlPathIdentifier r = (SqlPathIdentifier)this.getRight();
//    r.setSqlPathJoin(this);
  }

  private static SqlNode createLeft(List<String> names, SqlParserPos pos,
      List<SqlParserPos> componentPositions) {
    if (names.size() > 2) {
      return new SqlPathJoin(names.subList(0, names.size() - 1), pos,
          componentPositions.subList(0, names.size() - 1));
    }

    return new SqlIdentifier(names.subList(0, 1), null, pos, componentPositions.subList(0, 1));
  }

  private static SqlIdentifier createRight(List<String> names, SqlParserPos pos,
      List<SqlParserPos> componentPositions) {
    return new SqlIdentifier(names.subList(0, names.size()), null, pos,
        componentPositions.subList(0, names.size()));
  }
}
