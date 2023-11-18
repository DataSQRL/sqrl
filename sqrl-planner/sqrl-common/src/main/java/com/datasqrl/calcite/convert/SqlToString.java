package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SqlConverter.SqlNodes;

public interface SqlToString {
  SqlStrings convert(SqlNodes sqlNode);

  Dialect getDialect();

  interface SqlStrings {
    String getSql();
  }
}
