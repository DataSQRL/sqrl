package com.datasqrl.function.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

import com.datasqrl.flinkrunner.functions.json.JsonFunctions;
import com.datasqrl.function.translations.SnowflakeSqlTranslation;
//Disabled for now
//@AutoService(SqlTranslation.class)
public class JsonConcatSqlTranslation extends SnowflakeSqlTranslation {

  public JsonConcatSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_CONCAT));
  }

  /**
   * OBJECT_INSERT or ARRAY_APPEND depending if its an array or object
   */
  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    throw new RuntimeException("Cannot target json_concat on snowflake");
  }
}
