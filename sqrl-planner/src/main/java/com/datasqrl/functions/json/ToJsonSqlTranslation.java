package com.datasqrl.functions.json;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.json.JsonFunctions;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class ToJsonSqlTranslation extends PostgresSqlTranslation {

  public ToJsonSqlTranslation() {
    super(lightweightOp(JsonFunctions.TO_JSON));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, call.getOperandList().get(0),
        SqlLiteral.createSymbol(CastToJsonb.JSONB, SqlParserPos.ZERO))
        .unparse(writer, leftPrec, rightPrec);
  }

  enum CastToJsonb {
    JSONB
  }
}
