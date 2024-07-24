package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;

@Getter
public abstract class DialectSqlTranslation implements SqlTranslation {

  private final Dialect dialect;
  private final SqlOperator operator;

  public DialectSqlTranslation(Dialect dialect, SqlOperator operator) {
    this.dialect = dialect;
    this.operator = operator;
  }
}
