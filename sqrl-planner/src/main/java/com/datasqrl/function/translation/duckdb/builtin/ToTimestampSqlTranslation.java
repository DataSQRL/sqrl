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
package com.datasqrl.function.translation.duckdb.builtin;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translation.DuckDbSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class ToTimestampSqlTranslation extends DuckDbSqlTranslation {

  public ToTimestampSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("TO_TIMESTAMP"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var operandCount = call.operandCount();
    var operands = new ArrayList<>(call.getOperandList());

    if (operandCount == 1) {
      var cast = writer.startFunCall("CAST");
      operands.get(0).unparse(writer, 0, 0);
      writer.print(" AS TIMESTAMP");
      writer.endFunCall(cast);

    } else if (operandCount >= 2 && operands.get(1) instanceof SqlLiteral literal) {
      var duckDbPattern = DuckDbSqlTranslationUtils.flinkDateFormatToDuckDb(literal);
      operands.set(1, SqlLiteral.createCharString(duckDbPattern, SqlParserPos.ZERO));

      CalciteFunctionUtil.writeFunction("strptime", writer, operands);
    }
  }
}
