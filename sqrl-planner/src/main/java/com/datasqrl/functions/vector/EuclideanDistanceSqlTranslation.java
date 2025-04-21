package com.datasqrl.functions.vector;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.types.vector.functions.VectorFunctions;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class EuclideanDistanceSqlTranslation extends PostgresSqlTranslation {

  public EuclideanDistanceSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp(VectorFunctions.EUCLIDEAN_DISTANCE));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    PgSpecificOperatorTable.EuclideanDistance.createCall(SqlParserPos.ZERO, call.getOperandList())
        .unparse(writer, leftPrec, rightPrec);
  }
}
