package com.datasqrl.function.translations;

import org.apache.calcite.sql.SqlOperator;

import com.datasqrl.calcite.Dialect;

import lombok.Getter;

@Getter
public abstract class PostgresSqlTranslation extends DialectSqlTranslation {

  public PostgresSqlTranslation(SqlOperator operator) {
    super(Dialect.POSTGRES, operator);
  }
}
