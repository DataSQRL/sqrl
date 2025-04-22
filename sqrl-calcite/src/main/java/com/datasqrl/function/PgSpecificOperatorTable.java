package com.datasqrl.function;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A lightweight ts vector operator table
 */
public class PgSpecificOperatorTable {
  public static final SqlUnresolvedFunction TO_TSVECTOR = lightweightOp("to_tsvector");
  public static final SqlUnresolvedFunction TO_TSQUERY = lightweightOp("to_tsquery");
  public static final SqlUnresolvedFunction TO_WEBQUERY = lightweightOp("websearch_to_tsquery");
  public static final SqlUnresolvedFunction TS_RANK_CD = lightweightOp("ts_rank_cd");

  public static final SqlBinaryOperator MATCH = new SqlBinaryOperator("@@",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.BOOLEAN),
      null, null);
  public static final SqlBinaryOperator CosineDistance = new SqlBinaryOperator("<=>",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.DOUBLE),
      null, null);
  public static final SqlBinaryOperator EuclideanDistance = new SqlBinaryOperator("<->",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.DOUBLE),
      null, null);

  public static final SqlBinaryOperator JsonToString = new SqlBinaryOperator("#>>",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.ANY),
      null, null);
}
