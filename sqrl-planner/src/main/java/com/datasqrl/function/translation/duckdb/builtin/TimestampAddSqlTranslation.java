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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Translates Flink/Calcite {@code TIMESTAMPADD(unit, n, ts)} into DuckDB interval arithmetic. */
@AutoService(SqlTranslation.class)
public class TimestampAddSqlTranslation extends DuckDbSqlTranslation {

  public TimestampAddSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("TIMESTAMPADD"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var unitNode = call.operand(0);
    var nNode = call.operand(1);
    var tsNode = call.operand(2);

    var unit = DuckDbSqlTranslationUtils.extractTimeUnit(unitNode);

    // DuckDB: ts + n * INTERVAL '1 unit'
    // Special-case QUARTER as 3 months.
    writer.print("(");
    tsNode.unparse(writer, 0, 0);
    writer.print(" + ");

    if ("quarter".equals(unit)) {
      writer.print("(");
      nNode.unparse(writer, 0, 0);
      writer.print(" * 3)");
      writer.print(" * ");
      writeInterval(writer, "month");
    } else {
      nNode.unparse(writer, 0, 0);
      writer.print(" * ");
      writeInterval(writer, unit);
    }

    writer.print(")");
  }

  private static void writeInterval(SqlWriter writer, String unit) {
    writer.keyword("INTERVAL");
    SqlLiteral.createCharString("1 " + unit, SqlParserPos.ZERO).unparse(writer, 0, 0);
  }
}
