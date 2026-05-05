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

import static com.datasqrl.function.translation.duckdb.builtin.DuckDbSqlTranslationUtils.writeNullIfEmptyVarchar;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translation.DuckDbSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

@AutoService(SqlTranslation.class)
public class DateFormatSqlTranslation extends DuckDbSqlTranslation {

  public DateFormatSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("DATE_FORMAT"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    if (call.operandCount() != 2) {
      // Should not happen
      throw new UnsupportedOperationException("DATE_FORMAT requires 2 operands");
    }

    // Flink DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') uses Java-style patterns.
    // DuckDB strftime(ts, '%Y-%m-%d %H:%M:%S') uses strftime-style patterns.
    var duckDbPattern = DuckDbSqlTranslationUtils.flinkDateFormatToDuckDb(call.operand(1));

    var fn = writer.startFunCall("strftime");
    writeTimestampCast(writer, call.operand(0));
    writer.sep(",", true);
    SqlLiteral.createCharString(duckDbPattern, SqlParserPos.ZERO).unparse(writer, 0, 0);
    writer.endFunCall(fn);
  }

  private static void writeTimestampCast(SqlWriter writer, SqlNode operand) {
    var cast = writer.startFunCall("CAST");
    writeNullIfEmptyVarchar(writer, operand);
    writer.print(" AS TIMESTAMP");
    writer.endFunCall(cast);
  }
}
