package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;

public interface SqlNodeToString {
  SqlStrings convert(SqlNodes sqlNode);

  Dialect getDialect();

  interface SqlStrings {
    String getSql();
  }
}
