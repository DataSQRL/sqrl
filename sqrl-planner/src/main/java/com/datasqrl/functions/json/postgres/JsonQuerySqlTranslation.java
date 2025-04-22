package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class JsonQuerySqlTranslation extends PostgresSqlTranslation {

  public JsonQuerySqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_QUERY));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("jsonb_path_query").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
