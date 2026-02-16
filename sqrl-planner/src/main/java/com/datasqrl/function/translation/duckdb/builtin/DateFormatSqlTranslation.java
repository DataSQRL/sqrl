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
public class DateFormatSqlTranslation extends DuckDbSqlTranslation {

  public DateFormatSqlTranslation() {
    super(CalciteFunctionUtil.lightweightOp("DATE_FORMAT"));
  }

  @Override
  public void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec) {
    var operands = new ArrayList<>(call.getOperandList());

    // Flink DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') uses Java-style patterns.
    // DuckDB strftime(ts, '%Y-%m-%d %H:%M:%S') uses strftime-style patterns.
    if (operands.size() >= 2 && operands.get(1) instanceof SqlLiteral literal) {
      var format = String.valueOf(literal.toValue());
      operands.set(
          1, SqlLiteral.createCharString(flinkDateFormatToDuckDb(format), SqlParserPos.ZERO));
    }

    CalciteFunctionUtil.lightweightOp("strftime")
        .createCall(SqlParserPos.ZERO, operands)
        .unparse(writer, leftPrec, rightPrec);
  }

  static String flinkDateFormatToDuckDb(String flinkPattern) {
    if (flinkPattern == null || flinkPattern.isEmpty()) {
      return flinkPattern;
    }

    var out = new StringBuilder(flinkPattern.length() + 8);
    boolean inQuote = false;

    for (int i = 0; i < flinkPattern.length(); ) {
      char ch = flinkPattern.charAt(i);

      // Handle Java/SimpleDateFormat literal quoting using single quotes.
      if (ch == '\'') {
        if (i + 1 < flinkPattern.length() && flinkPattern.charAt(i + 1) == '\'') {
          appendLiteral(out, '\'');
          i += 2;
          continue;
        }
        inQuote = !inQuote;
        i++;
        continue;
      }

      if (inQuote) {
        appendLiteral(out, ch);
        i++;
        continue;
      }

      // Group repeated pattern letters.
      int j = i + 1;
      while (j < flinkPattern.length() && flinkPattern.charAt(j) == ch) {
        j++;
      }
      int count = j - i;

      String replacement =
          switch (ch) {
            case 'y' -> count == 2 ? "%y" : "%Y";
            case 'M' -> {
              if (count >= 4) {
                yield "%B";
              } else if (count == 3) {
                yield "%b";
              }
              yield "%m";
            }
            case 'd' -> "%d";
            case 'H' -> "%H";
            case 'h' -> "%I";
            case 'm' -> "%M";
            case 's' -> "%S";
            case 'S' -> "%f";
            case 'E' -> count >= 4 ? "%A" : "%a";
            case 'a' -> "%p";
            case 'Z', 'X' -> "%z";
            default -> null;
          };

      if (replacement != null) {
        out.append(replacement);
      } else {
        // Non-pattern characters are passed through as literals.
        for (int k = 0; k < count; k++) {
          appendLiteral(out, ch);
        }
      }

      i = j;
    }

    return out.toString();
  }

  private static void appendLiteral(StringBuilder out, char ch) {
    // DuckDB strftime uses % escapes; a literal % must become %%.
    if (ch == '%') {
      out.append("%%");
    } else {
      out.append(ch);
    }
  }
}
