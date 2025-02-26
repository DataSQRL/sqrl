package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.json.JsonFunctions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

// Disabled for now
// @AutoService(SqlTranslation.class)
public class JsonExistsAggSqlTranslation extends SnowflakeSqlTranslation {

  public JsonExistsAggSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_EXISTS));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    throw new RuntimeException("Cannot target json_exists on snowflake");
  }
}
