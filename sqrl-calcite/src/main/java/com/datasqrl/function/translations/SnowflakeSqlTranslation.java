package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;

@Getter
public abstract class SnowflakeSqlTranslation extends AbstractDialectSqlTranslation {
  public SnowflakeSqlTranslation(SqlOperator operator) {
    super(Dialect.SNOWFLAKE, operator);
  }
}
