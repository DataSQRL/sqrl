package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
//Disabled for now
//@AutoService(SqlTranslation.class)
public class JsonArraySqlTranslation extends SnowflakeSqlTranslation {

  public JsonArraySqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_ARRAY));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("ARRAY_CONSTRUCT").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
