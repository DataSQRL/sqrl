package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightAggOp;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.json.JsonFunctions;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class JsonObjectAggSqlTranslation extends PostgresSqlTranslation.Simple {

  public JsonObjectAggSqlTranslation() {
    super(JsonFunctions.JSON_OBJECTAGG, "jsonb_object_agg");
  }
}
