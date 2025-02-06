package com.datasqrl.function.translations;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

import com.datasqrl.calcite.Dialect;

public interface SqlTranslation {
  Dialect getDialect();
  SqlOperator getOperator();
  void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec);
}
