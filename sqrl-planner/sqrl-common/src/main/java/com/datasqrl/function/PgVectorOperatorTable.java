package com.datasqrl.function;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class PgVectorOperatorTable {
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