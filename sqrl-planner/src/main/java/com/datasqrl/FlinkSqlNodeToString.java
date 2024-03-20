package com.datasqrl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.google.auto.service.AutoService;

@AutoService(SqlNodeToString.class)
public class FlinkSqlNodeToString implements SqlNodeToString {

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
