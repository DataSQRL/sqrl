package com.datasqrl.function;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * A lightweight ts vector operator table
 */
public class PgSpecificOperatorTable {
  public static final SqlUnresolvedFunction TO_TSVECTOR = op("to_tsvector");
  public static final SqlUnresolvedFunction TO_TSQUERY = op("to_tsquery");
  public static final SqlUnresolvedFunction TO_WEBQUERY = op("websearch_to_tsquery");
  public static final SqlUnresolvedFunction TS_RANK_CD = op("ts_rank_cd");

  public static final SqlBinaryOperator MATCH = new SqlBinaryOperator("@@",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.BOOLEAN),
      null, null);
  public static final SqlBinaryOperator CosineDistance = new SqlBinaryOperator("<=>",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.DOUBLE),
      null, null);
  public static final SqlBinaryOperator EuclideanDistance = new SqlBinaryOperator("<->",
      SqlKind.OTHER_FUNCTION, 22, true, ReturnTypes.explicit(SqlTypeName.DOUBLE),
      null, null);


  private static SqlUnresolvedFunction op(String name) {
    return new SqlUnresolvedFunction(new SqlIdentifier(name, SqlParserPos.ZERO),
        null, null, null,
        null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }
}
