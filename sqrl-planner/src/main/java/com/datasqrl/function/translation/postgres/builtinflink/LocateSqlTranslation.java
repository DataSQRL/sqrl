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
package com.datasqrl.function.translation.postgres.builtinflink;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translation.PostgresSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class LocateSqlTranslation extends PostgresSqlTranslation {

  public LocateSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("LOCATE"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var substring = call.getOperandList().get(0);
    var string = call.getOperandList().get(1);

    if (call.getOperandList().size() == 2) {
      CalciteFunctionUtil.lightweightOp("strpos")
          .createCall(SqlParserPos.ZERO, string, substring)
          .unparse(writer, leftPrec, rightPrec);
    } else {
      var startPos = call.getOperandList().get(2);
      var one = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
      var adjustedStartPos = SqlStdOperatorTable.MINUS.createCall(SqlParserPos.ZERO, startPos, one);
      var substringCall =
          CalciteFunctionUtil.lightweightOp("substring")
              .createCall(SqlParserPos.ZERO, string, startPos);
      var strposCall =
          CalciteFunctionUtil.lightweightOp("strpos")
              .createCall(SqlParserPos.ZERO, substringCall, substring);
      var resultWithoutOffset = strposCall;
      var zero = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);
      var isZero = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, strposCall, zero);
      var resultWithOffset =
          SqlStdOperatorTable.PLUS.createCall(SqlParserPos.ZERO, strposCall, adjustedStartPos);
      SqlStdOperatorTable.CASE
          .createCall(SqlParserPos.ZERO, isZero, zero, resultWithOffset)
          .unparse(writer, leftPrec, rightPrec);
    }
  }
}
