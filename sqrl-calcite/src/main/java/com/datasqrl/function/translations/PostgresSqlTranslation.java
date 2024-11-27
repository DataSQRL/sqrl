package com.datasqrl.function.translations;

import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

@Getter
public abstract class PostgresSqlTranslation extends DialectSqlTranslation {

  public PostgresSqlTranslation(SqlOperator operator) {
    super(PostgresqlSqlDialect.DEFAULT, operator);
  }
}
