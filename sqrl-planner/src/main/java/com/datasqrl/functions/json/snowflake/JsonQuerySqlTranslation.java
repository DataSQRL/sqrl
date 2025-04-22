package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;

import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
//Disabled for now
//@AutoService(SqlTranslation.class)
public class JsonQuerySqlTranslation extends SnowflakeSqlTranslation {

  public JsonQuerySqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_QUERY));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    throw new RuntimeException("Cannot target json_query on snowflake");
  }
}
