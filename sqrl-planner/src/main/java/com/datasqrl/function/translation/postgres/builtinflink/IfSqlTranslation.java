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
import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class IfSqlTranslation extends PostgresSqlTranslation {

  public IfSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("IF"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var condition = call.operand(0);
    var trueVal = call.operand(1);
    var falseVal = call.operand(2);

    // CASE operator expects: [value, whenList, thenList, elseExpr]
    // For searched CASE (no value): [NULL, whenList, thenList, elseExpr]
    SqlStdOperatorTable.CASE
        .createCall(
            SqlParserPos.ZERO,
            null, // No value expr
            new SqlNodeList(List.of(condition), SqlParserPos.ZERO), // WHEN conditions
            new SqlNodeList(List.of(trueVal), SqlParserPos.ZERO), // THEN values
            falseVal) // ELSE value
        .unparse(writer, leftPrec, rightPrec);
  }
}
