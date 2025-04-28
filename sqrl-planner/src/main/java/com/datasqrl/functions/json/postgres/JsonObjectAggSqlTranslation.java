package com.datasqrl.functions.json.postgres;

import com.datasqrl.flinkrunner.functions.json.JsonFunctions;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class JsonObjectAggSqlTranslation extends PostgresSqlTranslation.Simple {

  public JsonObjectAggSqlTranslation() {
    super(JsonFunctions.JSON_OBJECTAGG, "jsonb_object_agg");
  }
}
