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
package com.datasqrl.function.translation.postgres.vector;

import com.datasqrl.flinkrunner.stdlib.vector.VectorFunctions;
import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.PgSpecificOperatorTable;
import com.datasqrl.function.translation.PostgresSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class CosineSimilaritySqlTranslation extends PostgresSqlTranslation {

  public CosineSimilaritySqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp(VectorFunctions.COSINE_SIMILARITY));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    SqlStdOperatorTable.MINUS
        .createCall(
            SqlParserPos.ZERO,
            SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
            PgSpecificOperatorTable.CosineDistance.createCall(
                SqlParserPos.ZERO, call.getOperandList()))
        .unparse(writer, leftPrec, rightPrec);
  }
}
