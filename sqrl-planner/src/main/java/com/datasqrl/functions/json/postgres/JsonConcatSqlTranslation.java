package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightBiOp;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class JsonConcatSqlTranslation extends PostgresSqlTranslation {

  public JsonConcatSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_CONCAT));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightBiOp("||").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
