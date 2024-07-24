package com.datasqrl.functions.json.snowflake;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SnowflakeSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.json.JsonFunctions;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class JsonExistsAggSqlTranslation extends SnowflakeSqlTranslation {

  public JsonExistsAggSqlTranslation() {
    super(lightweightOp(JsonFunctions.JSON_EXISTS));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    throw new RuntimeException("Cannot target json_exists on snowflake");
  }
}
