package com.datasqrl.calcite.convert;

import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.google.auto.service.AutoService;

@AutoService(SqlNodeToString.class)
public class SnowflakeSqlNodeToString implements SqlNodeToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    var config = SqlPrettyWriter.config()
        .withDialect(ExtendedSnowflakeSqlDialect.DEFAULT)
        .withQuoteAllIdentifiers(false)
        .withIndentation(0);
    var prettyWriter = new SqlPrettyWriter(config);
    sqlNode.getSqlNode().unparse(prettyWriter, 0, 0);
    return () -> prettyWriter.toSqlString().getSql();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.SNOWFLAKE;
  }
}
