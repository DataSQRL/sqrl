package com.datasqrl.calcite.sql;

import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class PostgresDynamicParameterStrategy implements DynamicParameterStrategy {

  @Override
  public void apply(SqlPrettyWriter sqlWriter, int index) {
    sqlWriter.print("$" + (index + 1));
  }
}
