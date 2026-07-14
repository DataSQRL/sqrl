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
import java.math.BigDecimal;
import java.util.ArrayList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.FunctionDefinition;

class AbstractSubstringSqlTranslation extends DuckDbSqlTranslation {

  private final String targetFnName;

  protected AbstractSubstringSqlTranslation(
      FunctionDefinition functionDefinition, String targetFnName) {
    super(functionDefinition);
    this.targetFnName = targetFnName;
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var operands = new ArrayList<>(call.getOperandList());
    operands.set(1, rewriteZeroStart(operands.get(1)));

    CalciteFunctionUtil.writeFunction(targetFnName, writer, operands);
  }

  private SqlNode rewriteZeroStart(SqlNode start) {
    if (start instanceof SqlLiteral literal
        && literal.bigDecimalValue().compareTo(BigDecimal.ZERO) == 0) {
      return SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);
    }

    return start;
  }
}
