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
package com.datasqrl.planner.util;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlTableNameExtractor {

  /** Regex that captures a single identifier in any quoting style. */
  private static final String IDENTIFIER =
      "(?:`([^`]+)`|\"([^\"]+)\"|\\[([^\\]]+)\\]|([a-zA-Z_][a-zA-Z0-9_$]*))";

  /** FROM / JOIN pattern (same as before, minus compile in loop). */
  private static final Pattern TABLE_PATTERN =
      Pattern.compile(
          "\\b(?:from|join)\\s+"
              + "(?!\\(\\s*select)"
              + // negative lookahead to skip subqueries
              IDENTIFIER,
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** CTE definition pattern: WITH [RECURSIVE] <ident> AS ( … ) [, <ident> AS ( … )]* */
  private static final Pattern CTE_PATTERN =
      Pattern.compile(
          "\\bwith\\s+(?:recursive\\s+)?"
              + IDENTIFIER
              + "\\s+as\\s*\\("
              + "(?:[^()]*+|\\((?:[^()]*+|\\([^()]*+\\))*+\\))*+"
              + "\\)"
              + "(?:\\s*,\\s*"
              + IDENTIFIER
              + "\\s+as\\s*\\("
              + "(?:[^()]*+|\\((?:[^()]*+|\\([^()]*+\\))*+\\))*+\\))*",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * Extracts all the table names from a given SQL string (but excluding CTEs defined in the
   * string).
   *
   * <p>Assumptions
   *
   * <ul>
   *   <li>Each table is referenced by a single identifier (no db.schema.table).
   *   <li>Identifiers may be bare (<code>orders</code>) or quoted with back-ticks (<code>`Orders`
   *       </code>), double-quotes (<code>"Orders"</code>), or square brackets (<code>[Orders]
   *       </code>).
   *   <li>SQL comments (<code>-- …</code> &amp; /* ..) and string literals are ignored to prevent
   *       false positives.
   * </ul>
   *
   * @param sql Arbitrary SQL statement (may contain multiple sub-queries)
   * @return Set of distinct table names as they appear in the query (quotes removed)
   */
  public static Set<String> findTableNames(String sql) {
    Objects.requireNonNull(sql, "sql");

    String sanitized = stripLiteralsAndComments(sql).replaceAll("\\s+", " ");

    // 1. Collect names defined in the WITH-clause (CTEs)
    Set<String> cteNames = extractCteNames(sanitized);

    // 2. Collect everything that sits after FROM / JOIN
    Set<String> all = extractFromJoinIdentifiers(sanitized);

    // 3. Remove the CTE names – what is left are the base tables
    all.removeAll(cteNames);
    return all;
  }

  private static Set<String> extractFromJoinIdentifiers(String sql) {
    Set<String> tables = new LinkedHashSet<>();

    // First, extract table names from subqueries by recursively processing them
    Pattern subqueryPattern =
        Pattern.compile(
            "\\b(?:from|join)\\s+\\(\\s*(select.*?)\\)\\s*(?:as\\s+)?\\w*\\s*",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    Matcher subqueryMatcher = subqueryPattern.matcher(sql);
    while (subqueryMatcher.find()) {
      String subquery = subqueryMatcher.group(1);
      tables.addAll(extractFromJoinIdentifiers(subquery)); // Recursive call
    }

    // Then extract direct table references (excluding subqueries)
    Matcher m = TABLE_PATTERN.matcher(sql);
    while (m.find()) {
      addFirstNonNullGroup(tables, m, 4);
    }
    return tables;
  }

  private static Set<String> extractCteNames(String sql) {
    Set<String> ctes = new HashSet<>();
    Matcher m = CTE_PATTERN.matcher(sql);
    while (m.find()) {
      // The pattern captures one or more identifiers; step through all captures
      for (int g = 1; g <= 4; g++) addFirstNonNullGroup(ctes, m, g);
      // For additional CTEs in the same WITH, groups repeat after the big match.
      for (int g = 5; g <= m.groupCount(); g += 4) {
        for (int sub = g; sub < g + 4; sub++) {
          String id = m.group(sub);
          if (id != null) {
            ctes.add(stripQuotes(id));
            break;
          }
        }
      }
    }
    return ctes;
  }

  /* --------------------- utility helpers ----------------------------- */

  private static void addFirstNonNullGroup(Set<String> out, Matcher m, int maxGroup) {
    for (int g = 1; g <= maxGroup; g++) {
      String id = m.group(g);
      if (id != null) {
        out.add(stripQuotes(id));
        break;
      }
    }
  }

  private static String stripQuotes(String s) {
    // Caller guarantees no mixed quoting, so trim leading/trailing quote char.
    if (s.length() > 1 && ("`\"[".indexOf(s.charAt(0)) >= 0)) {
      return s.substring(1, s.length() - 1);
    }
    return s;
  }

  private static String stripLiteralsAndComments(String s) {
    // Remove single‑line comments
    s = s.replaceAll("(?m)--.*?$", " ");
    // Remove multi‑line comments
    s = s.replaceAll("/\\*.*?\\*/", " ");
    // Remove single‑quoted string literals
    s = s.replaceAll("'(?:''|[^'])*'", " ");
    // Remove PostgreSQL dollar‑quoted literals
    s = s.replaceAll("(?s)\\$\\$.*?\\$\\$", " ");
    return s;
  }
}
