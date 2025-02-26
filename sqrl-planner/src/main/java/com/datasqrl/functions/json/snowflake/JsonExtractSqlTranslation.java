package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.json.JsonFunctions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

// Disabled for now
// @AutoService(SqlTranslation.class)
public class JsonExtractSqlTranslation extends SnowflakeSqlTranslation {

  public JsonExtractSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_EXTRACT));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    throw new RuntimeException("Cannot target json_exists on snowflake");
  }
}
