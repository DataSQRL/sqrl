package com.datasqrl.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class SqlNodePrinter {

  public static String printJoin(SqlNode from) {
    SqlPrettyWriter sqlWriter = new SqlPrettyWriter();
    sqlWriter.startList("", "");
    from.unparse(sqlWriter, 0, 0);
    return sqlWriter.toString();
  }
}
