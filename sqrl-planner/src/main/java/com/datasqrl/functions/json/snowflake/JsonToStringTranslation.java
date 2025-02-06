package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.json.JsonFunctions;
//Disabled for now
//@AutoService(SqlTranslation.class)
public class JsonToStringTranslation extends SnowflakeSqlTranslation {

  public JsonToStringTranslation() {
    super(lightweightOp(JsonFunctions.JSON_TO_STRING));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("TO_JSON").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
