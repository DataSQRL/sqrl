package com.datasqrl.functions.json.postgres;

import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.types.json.functions.JsonFunctions;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class JsonObjectAggSqlTranslation extends PostgresSqlTranslation.Simple {

  public JsonObjectAggSqlTranslation() {
    super(JsonFunctions.JSON_OBJECTAGG, "jsonb_object_agg");
  }
}
