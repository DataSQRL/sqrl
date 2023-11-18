package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;

@AllArgsConstructor
@Getter
public abstract class PostgresSqlTranslation implements SqlTranslation {
  SqlOperator operator;

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }
}
