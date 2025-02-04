package com.datasqrl.functions.vector;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.vector.VectorFunctions;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class CosineDistanceSqlTranslation extends PostgresSqlTranslation.Simple {

  public CosineDistanceSqlTranslation() {
    super(VectorFunctions.COSINE_DISTANCE, PgSpecificOperatorTable.CosineDistance);
  }
}
