package com.datasqrl.calcite.convert;

import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.DynamicParamSqlPrettyWriter;
import com.datasqrl.calcite.SqrlConfigurations;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.google.auto.service.AutoService;

@AutoService(SqlNodeToString.class)
public class PostgresSqlNodeToString implements SqlNodeToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    var config = SqrlConfigurations.sqlToString.apply(SqlPrettyWriter.config()
        .withDialect(ExtendedPostgresSqlDialect.DEFAULT));
    var writer = new DynamicParamSqlPrettyWriter(config);
    sqlNode.getSqlNode().unparse(writer, 0, 0);
    return ()->writer.toSqlString().getSql();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }
}
