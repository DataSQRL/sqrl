package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.json.JsonFunctions;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class ToJsonSqlTranslation extends PostgresSqlTranslation {

  public ToJsonSqlTranslation() {
    super(lightweightOp(JsonFunctions.TO_JSON));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, call.getOperandList().getFirst(),
        SqlLiteral.createSymbol(CastToJsonb.JSONB, SqlParserPos.ZERO))
        .unparse(writer, leftPrec, rightPrec);
  }

  enum CastToJsonb {
    JSONB
  }
}
