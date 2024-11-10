package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;

@Getter
@AllArgsConstructor
public abstract class DialectSqlTranslation implements SqlTranslation {

  private final SqlDialect sqlDialect;
  private final SqlOperator operator;
}
