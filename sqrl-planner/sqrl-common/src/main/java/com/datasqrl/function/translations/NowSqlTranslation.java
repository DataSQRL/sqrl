package com.datasqrl.function.translations;

import com.datasqrl.DefaultFunctions;
import com.datasqrl.calcite.Dialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class NowSqlTranslation extends PostgresSqlTranslation {

  public NowSqlTranslation() {
    super(DefaultFunctions.NOW);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.CURRENT_TIMESTAMP.createCall(SqlParserPos.ZERO)
        .unparse(writer, leftPrec, rightPrec);
  }
}
