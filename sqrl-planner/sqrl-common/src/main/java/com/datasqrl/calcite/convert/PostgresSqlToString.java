package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.DynamicParamSqlPrettyWriter;
import com.datasqrl.calcite.SqrlConfigurations;
import com.datasqrl.calcite.convert.SqlConverter.SqlNodes;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@AutoService(SqlToString.class)
public class PostgresSqlToString implements SqlToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    SqlWriterConfig config = SqrlConfigurations.sqlToString.apply(SqlPrettyWriter.config());
    DynamicParamSqlPrettyWriter writer = new DynamicParamSqlPrettyWriter(config);
    sqlNode.getSqlNode().unparse(writer, 0, 0);
    return ()->writer.toSqlString().getSql();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }
}
