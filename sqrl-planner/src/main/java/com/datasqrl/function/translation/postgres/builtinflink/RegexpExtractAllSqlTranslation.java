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

import static org.apache.calcite.sql.parser.SqlParserPos.*;

import com.datasqrl.function.CalciteFunctionUtil;
import com.datasqrl.function.translation.PostgresSqlTranslation;
import com.datasqrl.function.translation.SqlTranslation;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;

@AutoService(SqlTranslation.class)
public class RegexpExtractAllSqlTranslation extends PostgresSqlTranslation {

  public RegexpExtractAllSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("REGEXP_EXTRACT_ALL"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var str = call.operand(0);
    var regex = call.operand(1);

    // Determine extract index (default 1)
    int extractIndex = 1;
    if (call.operandCount() > 2) {
      var indexOperand = call.operand(2);
      if (indexOperand instanceof SqlLiteral literal) {
        extractIndex = literal.intValue(false);
      }
    }

    // PostgreSQL: ARRAY(SELECT m[extractIndex] FROM regexp_matches(str, regex, 'g') AS m)
    writer.keyword("ARRAY");
    var arrayParen = writer.startList("(", ")");

    // SELECT m[extractIndex]
    writer.keyword("SELECT");
    writer.print(" m");
    var brackets = writer.startList("[", "]");
    writer.print(String.valueOf(extractIndex));
    writer.endList(brackets);

    // FROM regexp_matches(str, regex, 'g') AS m
    writer.keyword("FROM");
    writer.print(" ");
    CalciteFunctionUtil.writeFunction(
        "REGEXP_MATCHES", writer, str, regex, SqlLiteral.createCharString("g", ZERO));
    writer.print(" AS m");

    writer.endList(arrayParen);
  }
}
