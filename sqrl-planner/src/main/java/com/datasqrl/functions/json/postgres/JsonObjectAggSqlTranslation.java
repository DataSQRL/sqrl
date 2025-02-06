package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightAggOp;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.json.JsonFunctions;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class JsonObjectAggSqlTranslation extends PostgresSqlTranslation {

  public JsonObjectAggSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_OBJECTAGG));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightAggOp("jsonb_object_agg").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
