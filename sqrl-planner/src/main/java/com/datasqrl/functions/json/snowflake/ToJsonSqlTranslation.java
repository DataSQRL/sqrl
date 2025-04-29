package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.flinkrunner.functions.json.JsonFunctions;
import com.datasqrl.function.translations.SnowflakeSqlTranslation;
//Disabled for now
//@AutoService(SqlTranslation.class)
public class ToJsonSqlTranslation extends SnowflakeSqlTranslation {

  public ToJsonSqlTranslation() {
    super(lightweightOp(JsonFunctions.TO_JSON));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    lightweightOp("PARSE_JSON").createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }

}
