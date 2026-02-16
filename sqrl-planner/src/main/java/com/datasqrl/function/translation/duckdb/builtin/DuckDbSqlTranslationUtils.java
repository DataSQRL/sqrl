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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;

final class DuckDbSqlTranslationUtils {

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
