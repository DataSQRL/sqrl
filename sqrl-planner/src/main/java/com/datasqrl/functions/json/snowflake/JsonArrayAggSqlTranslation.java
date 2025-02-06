package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightAggOp;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.json.JsonFunctions;

//Disabled for now
//@AutoService(SqlTranslation.class)
public class JsonArrayAggSqlTranslation extends SnowflakeSqlTranslation {

  public JsonArrayAggSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_ARRAYAGG));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightAggOp("ARRAY_AGG").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
