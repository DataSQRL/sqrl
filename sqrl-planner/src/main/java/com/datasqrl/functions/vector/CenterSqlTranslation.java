package com.datasqrl.functions.vector;

import static com.datasqrl.function.DocumentedFunction.getFunctionNameFromClass;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.vector.Center;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class CenterSqlTranslation extends PostgresSqlTranslation {

  public CenterSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp(
        getFunctionNameFromClass(Center.class)));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.AVG.createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
