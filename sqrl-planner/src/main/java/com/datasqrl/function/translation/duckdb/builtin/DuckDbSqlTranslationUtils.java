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

import java.math.BigDecimal;
import java.util.Map;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

final class DuckDbSqlTranslationUtils {

  /**
   * Maps a {@code (innerUnit, divisor)} pair to the DuckDB time unit it represents. The outer key
   * is the unit of the value being divided:
   *
   * <ul>
   *   <li>{@code "millisecond"} / {@code "month"} for raw {@code Reinterpret(MINUS_DATE(…))} values
   *       — Calcite stores day-time intervals as ms and year-month intervals as months.
   *   <li>{@code "second"} / {@code "month"} for {@code date_diff(unit, …)} calls produced by an
   *       earlier rewrite pass — used to collapse Calcite's WEEK/QUARTER decomposition (which it
   *       emits as a SECOND/MONTH diff wrapped in an extra {@code DIVIDE_INTEGER}) back into a
   *       single native call.
   * </ul>
   */
  private static final Map<String, Map<BigDecimal, String>> DIVISOR_TO_UNIT =
      Map.of(
          "millisecond",
              Map.of(
                  new BigDecimal("86400000"), "day",
                  new BigDecimal("3600000"), "hour",
                  new BigDecimal("60000"), "minute",
                  new BigDecimal("1000"), "second"),
          "second", Map.of(new BigDecimal("604800"), "week"),
          "month",
              Map.of(
                  new BigDecimal("12"), "year",
                  new BigDecimal("3"), "quarter"));

  /**
   * Returns the DuckDB time unit produced by dividing a value of {@code innerUnit} by {@code
   * divisor}, or {@code null} if the pair is not a known time-unit conversion.
   */
  static String divisorToTimeUnit(String innerUnit, BigDecimal divisor) {
    return DIVISOR_TO_UNIT.getOrDefault(innerUnit, Map.of()).get(divisor);
  }

  /** Converts a Calcite {@link TimeUnit} enum to a DuckDB time unit string. */
  static String extractTimeUnit(TimeUnit timeUnit) {
    return extractTimeUnit(timeUnit.name());
  }

  static String extractTimeUnit(SqlNode timeUnitNode) {
    // TIMESTAMPADD accepts a time unit token (e.g. DAY, HOUR).
    // Depending on how the SQL was produced, this may be a SYMBOL literal, identifier, etc.
    String raw;
    if (timeUnitNode instanceof SqlLiteral literal) {
      var val = literal.toValue();
      raw = val == null ? "" : val;
    } else if (timeUnitNode instanceof SqlIdentifier identifier) {
      raw = identifier.toString();
    } else {
      raw = timeUnitNode.toString();
    }

    raw = raw.trim();
    if (raw.regionMatches(true, 0, "SQL_TSI_", 0, "SQL_TSI_".length())) {
      raw = raw.substring("SQL_TSI_".length());
    }

    return extractTimeUnit(raw);
  }

  static String flinkDateFormatToDuckDb(SqlLiteral patternLiteral) {
    var flinkPattern = patternLiteral.toValue();

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

  static void writeNullIfEmptyVarchar(SqlWriter writer, SqlNode operand) {
    var nullIf = writer.startFunCall("NULLIF");
    var cast = writer.startFunCall("CAST");
    operand.unparse(writer, 0, 0);
    writer.print(" AS VARCHAR");
    writer.endFunCall(cast);
    writer.sep(",", true);
    SqlLiteral.createCharString("", SqlParserPos.ZERO).unparse(writer, 0, 0);
    writer.endFunCall(nullIf);
  }

  private static void appendLiteral(StringBuilder out, char ch) {
    // DuckDB strftime uses % escapes; a literal % must become %%.
    if (ch == '%') {
      out.append("%%");
    } else {
      out.append(ch);
    }
  }

  private static String extractTimeUnit(String raw) {
    return switch (raw.toUpperCase()) {
      case "YEAR" -> "year";
      case "QUARTER" -> "quarter";
      case "MONTH" -> "month";
      case "WEEK" -> "week";
      case "DAY" -> "day";
      case "HOUR" -> "hour";
      case "MINUTE" -> "minute";
      case "SECOND" -> "second";
      case "MILLISECOND" -> "millisecond";
      case "MICROSECOND" -> "microsecond";
      case "NANOSECOND" -> "nanosecond";
      default ->
          throw new UnsupportedOperationException(
              "Unsupported DuckDB time unit: %s".formatted(raw));
    };
  }
}
