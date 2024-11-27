package com.datasqrl.function.translations;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

public interface SqlTranslation {
  SqlDialect getSqlDialect();
  SqlOperator getOperator();
  void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec);
}
