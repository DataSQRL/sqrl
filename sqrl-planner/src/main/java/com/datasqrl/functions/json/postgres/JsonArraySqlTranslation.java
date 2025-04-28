package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.flinkrunner.functions.json.JsonFunctions;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class JsonArraySqlTranslation extends PostgresSqlTranslation {

  public JsonArraySqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_ARRAY));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("jsonb_build_array").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
