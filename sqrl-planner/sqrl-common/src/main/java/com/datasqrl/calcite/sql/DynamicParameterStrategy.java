package com.datasqrl.calcite.sql;

import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public interface DynamicParameterStrategy {

  void apply(SqlPrettyWriter sqlWriter, int index);
}
