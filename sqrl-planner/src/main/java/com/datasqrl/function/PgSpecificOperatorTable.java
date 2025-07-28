/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.function;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

/** A lightweight ts vector operator table */
public class PgSpecificOperatorTable {
  public static final SqlUnresolvedFunction TO_TSVECTOR = lightweightOp("to_tsvector");
  public static final SqlUnresolvedFunction TO_TSQUERY = lightweightOp("to_tsquery");
  public static final SqlUnresolvedFunction TO_WEBQUERY = lightweightOp("websearch_to_tsquery");
  public static final SqlUnresolvedFunction TS_RANK_CD = lightweightOp("ts_rank_cd");

  public static final SqlBinaryOperator MATCH =
      new SqlBinaryOperator(
          "@@",
          SqlKind.OTHER_FUNCTION,
          22,
          true,
          ReturnTypes.explicit(SqlTypeName.BOOLEAN),
          null,
          null);
  public static final SqlBinaryOperator CosineDistance =
      new SqlBinaryOperator(
          "<=>",
          SqlKind.OTHER_FUNCTION,
          22,
          true,
          ReturnTypes.explicit(SqlTypeName.DOUBLE),
          null,
          null);
  public static final SqlBinaryOperator EuclideanDistance =
      new SqlBinaryOperator(
          "<->",
          SqlKind.OTHER_FUNCTION,
          22,
          true,
          ReturnTypes.explicit(SqlTypeName.DOUBLE),
          null,
          null);

  public static final SqlBinaryOperator JsonToString =
      new SqlBinaryOperator(
          "#>>",
          SqlKind.OTHER_FUNCTION,
          22,
          true,
          ReturnTypes.explicit(SqlTypeName.ANY),
          null,
          null);

  public static final SqlBinaryOperator EqualsAny =
      new SqlBinaryOperator(
          "= ANY",
          SqlKind.OTHER_FUNCTION,
          22,
          true,
          ReturnTypes.explicit(SqlTypeName.BOOLEAN),
          null,
          null) {

        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          var left = call.operand(0);
          var right = call.operand(1);

          left.unparse(writer, leftPrec, getLeftPrec());
          writer.keyword("= ANY");
          writer.print("(");
          right.unparse(writer, getRightPrec(), rightPrec);
          writer.print(")");
        }
      };
}
