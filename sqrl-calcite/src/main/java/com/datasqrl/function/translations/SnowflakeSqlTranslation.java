package com.datasqrl.function.translations;

import org.apache.calcite.sql.SqlOperator;

import com.datasqrl.calcite.Dialect;

import lombok.Getter;

@Getter
public abstract class SnowflakeSqlTranslation extends AbstractDialectSqlTranslation {
  public SnowflakeSqlTranslation(SqlOperator operator) {
    super(Dialect.SNOWFLAKE, operator);
  }
}
