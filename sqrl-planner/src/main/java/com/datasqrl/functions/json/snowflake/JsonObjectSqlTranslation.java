package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.json.JsonFunctions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

// Disabled for now
// @AutoService(SqlTranslation.class)
public class JsonObjectSqlTranslation extends SnowflakeSqlTranslation {

  public JsonObjectSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_OBJECT));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("OBJECT_CONSTRUCT")
        .createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
