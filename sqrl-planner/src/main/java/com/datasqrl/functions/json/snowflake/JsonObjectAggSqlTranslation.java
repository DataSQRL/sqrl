package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightAggOp;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
//Disabled for now
//@AutoService(SqlTranslation.class)
public class JsonObjectAggSqlTranslation extends SnowflakeSqlTranslation {

  public JsonObjectAggSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_OBJECTAGG));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightAggOp("OBJECT_AGG").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
