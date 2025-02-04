package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.CalciteFunctionUtil;
import lombok.Getter;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;

@Getter
public abstract class AbstractDialectSqlTranslation implements SqlTranslation {

  private final Dialect dialect;
  private final SqlOperator operator;

  public AbstractDialectSqlTranslation(Dialect dialect, SqlOperator operator) {
    this.dialect = dialect;
    this.operator = operator;
  }

  public AbstractDialectSqlTranslation(Dialect dialect, FunctionDefinition operator) {
    this(dialect, CalciteFunctionUtil.lightweightOp(operator));
  }
}
