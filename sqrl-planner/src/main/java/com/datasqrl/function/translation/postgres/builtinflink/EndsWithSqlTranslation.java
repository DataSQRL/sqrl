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

import com.datasqrl.function.translation.PostgresSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

@AutoService(SqlTranslation.class)
public class EndsWithSqlTranslation extends PostgresSqlTranslation {

  public EndsWithSqlTranslation() {
    super(BuiltInFunctionDefinitions.ENDS_WITH);
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var expr = call.getOperandList().get(0);
    var suffix = call.getOperandList().get(1);

    var charLength = SqlStdOperatorTable.CHAR_LENGTH.createCall(SqlParserPos.ZERO, suffix);
    var right = FlinkSqlOperatorTable.RIGHT.createCall(SqlParserPos.ZERO, expr, charLength);

    SqlStdOperatorTable.EQUALS
        .createCall(SqlParserPos.ZERO, right, suffix)
        .unparse(writer, leftPrec, rightPrec);
  }
}
