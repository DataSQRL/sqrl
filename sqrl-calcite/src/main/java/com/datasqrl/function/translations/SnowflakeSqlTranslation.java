package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;

@Getter
public abstract class SnowflakeSqlTranslation extends DialectSqlTranslation {
  public SnowflakeSqlTranslation(SqlOperator operator) {
    super(ExtendedSnowflakeSqlDialect.DEFAULT, operator);
  }
}
