package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@AutoService(SqlNodeToString.class)
public class SnowflakeSqlNodeToString implements SqlNodeToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    SqlWriterConfig config = SqlPrettyWriter.config()
        .withDialect(ExtendedSnowflakeSqlDialect.DEFAULT)
        .withQuoteAllIdentifiers(false)
        .withIndentation(0);
    SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config);
    sqlNode.getSqlNode().unparse(prettyWriter, 0, 0);
    return () -> prettyWriter.toSqlString().getSql();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.SNOWFLAKE;
  }
}
