package com.datasqrl.vector;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translations.PostgresSqlTranslation;
import com.datasqrl.function.translations.SqlTranslation;
import com.google.auto.service.AutoService;
import java.math.BigDecimal;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class CosineSimilaritySqlTranslation extends PostgresSqlTranslation {

  public CosineSimilaritySqlTranslation() {
    super(lightweightOp(VectorFunctions.COSINE_SIMILARITY.getFunctionName().getCanonical()));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
        PgSpecificOperatorTable.CosineDistance.createCall(SqlParserPos.ZERO, call.getOperandList()))
        .unparse(writer, leftPrec, rightPrec);
  }
}
