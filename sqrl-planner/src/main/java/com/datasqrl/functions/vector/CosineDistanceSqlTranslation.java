package com.datasqrl.functions.vector;

import com.datasqrl.flinkrunner.functions.vector.VectorFunctions;
import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class CosineDistanceSqlTranslation extends PostgresSqlTranslation.Simple {

  public CosineDistanceSqlTranslation() {
    super(VectorFunctions.COSINE_DISTANCE, PgSpecificOperatorTable.CosineDistance);
  }
}
