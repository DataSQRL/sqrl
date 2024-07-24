package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;

@Getter
public abstract class PostgresSqlTranslation extends DialectSqlTranslation {

  public PostgresSqlTranslation(SqlOperator operator) {
    super(Dialect.POSTGRES, operator);
  }
}
