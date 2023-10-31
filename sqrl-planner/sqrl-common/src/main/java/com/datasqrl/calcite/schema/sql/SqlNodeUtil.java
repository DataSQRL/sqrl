package com.datasqrl.calcite.schema.sql;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlNodeUtil {

  public static SqlNode and(SqlParserPos pos, List<SqlNode> conditions) {
    if (conditions.size() == 0) {
      throw new RuntimeException("Cannot create condition");
    } else if (conditions.size() == 2) {
      return SqlStdOperatorTable.AND.createCall(pos, conditions.get(0), conditions.get(1));
    }

    return SqlStdOperatorTable.AND.createCall(pos, conditions.get(0),
        and(pos, conditions.subList(1, conditions.size())));
  }

}
