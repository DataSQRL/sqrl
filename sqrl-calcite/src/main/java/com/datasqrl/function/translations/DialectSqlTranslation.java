package com.datasqrl.function.translations;

import org.apache.calcite.sql.SqlOperator;

import com.datasqrl.calcite.Dialect;

import lombok.Getter;

@Getter
public abstract class DialectSqlTranslation implements SqlTranslation {

  private final Dialect dialect;
  private final SqlOperator operator;

  public DialectSqlTranslation(Dialect dialect, SqlOperator operator) {
    this.dialect = dialect;
    this.operator = operator;
  }
}
