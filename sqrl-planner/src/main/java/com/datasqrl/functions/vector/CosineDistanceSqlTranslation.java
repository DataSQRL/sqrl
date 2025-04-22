package com.datasqrl.functions.vector;

import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.types.vector.functions.VectorFunctions;
import com.google.auto.service.AutoService;

@AutoService(SqlTranslation.class)
public class CosineDistanceSqlTranslation extends PostgresSqlTranslation.Simple {

  public CosineDistanceSqlTranslation() {
    super(VectorFunctions.COSINE_DISTANCE, PgSpecificOperatorTable.CosineDistance);
  }
}
