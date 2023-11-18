package com.datasqrl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SqlConverter.SqlNodes;
import com.datasqrl.calcite.convert.SqlToString;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.google.auto.service.AutoService;

@AutoService(SqlToString.class)
public class FlinkSqlToString implements SqlToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    // TODO: Migrate remaining FlinkRelToSqlConverter to this paradigm
    return RelToFlinkSql.convertToString(sqlNode);
  }

  @Override
  public Dialect getDialect() {
    return Dialect.FLINK;
  }
}
