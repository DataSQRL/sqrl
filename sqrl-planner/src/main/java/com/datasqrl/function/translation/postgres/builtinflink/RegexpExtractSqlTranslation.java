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

@AutoService(SqlTranslation.class)
public class RegexpExtractSqlTranslation extends PostgresSqlTranslation {

  public RegexpExtractSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("REGEXP_EXTRACT"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var string = call.getOperandList().get(0);
    var regex = call.getOperandList().get(1);

    // Check if group index is provided
    boolean useRegexpSubstr;
    if (call.operandCount() > 2) {
      var groupIndex = call.operand(2);

      // Check if it's a literal 0 (which means full match)
      if (groupIndex instanceof SqlLiteral literal) {
        var indexValue = literal.intValue(false);
        useRegexpSubstr = (indexValue == 0);
      } else {
        // Non-literal index - must use array subscript approach
        useRegexpSubstr = false;
      }

      if (!useRegexpSubstr) {
        // For group >= 1: (regexp_match(string1, string2))[integer]
        var parenthesis = writer.startList("(", ")");
        CalciteFunctionUtil.writeFunction("REGEXP_MATCH", writer, string, regex);
        writer.endList(parenthesis);
        var brackets = writer.startList("[", "]");
        groupIndex.unparse(writer, 0, 0);
        writer.endList(brackets);
        return;
      }
    }

    // No group index or group index = 0 - use regexp_substr for full match
    CalciteFunctionUtil.writeFunction("REGEXP_SUBSTR", writer, string, regex);
  }
}
