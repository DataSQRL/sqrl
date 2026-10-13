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
package com.datasqrl.planner;

import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.planner.util.SqlTableNameExtractor;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Turns Flink parser and validator errors into SQRL errors with an error code and an actionable
 * hint, e.g. a "did you mean" suggestion for a misspelled table or column.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ErrorHints {

  private static final Pattern OBJECT_NOT_FOUND =
      Pattern.compile("(?:Object|Table) '([^']+)' not found");
  private static final Pattern COLUMN_NOT_FOUND = Pattern.compile("Column '([^']+)' not found");
  // A line inside a statement that starts another statement, i.e. the ';' before it is missing
  private static final Pattern STATEMENT_START =
      Pattern.compile(
          "\\n\\s*(?:/\\*\\+|IMPORT\\s|EXPORT\\s|CREATE\\s|[\\w.`]+\\s*(?:\\([^)]*\\))?\\s*:=)",
          Pattern.CASE_INSENSITIVE);

  public record Hint(ErrorLabel label, String message) {}

  /**
   * @param message error message of the Flink parser or validator
   * @param statement source of the failing statement
   * @param errorLine line of the error, relative to the statement
   * @param catalogManager catalog to look up table and column names
   */
  public static Hint explain(
      String message, String statement, int errorLine, CatalogManager catalogManager) {
    var objectMatcher = OBJECT_NOT_FOUND.matcher(message);
    if (objectMatcher.find()) {
      var suggestion = closestMatch(objectMatcher.group(1), tableNames(catalogManager));
      return new Hint(ErrorCode.MISSING_TABLE, withSuggestion(message, suggestion));
    }

    var columnMatcher = COLUMN_NOT_FOUND.matcher(message);
    if (columnMatcher.find()) {
      var suggestion = closestMatch(columnMatcher.group(1), columnNames(statement, catalogManager));
      return new Hint(ErrorCode.MISSING_FIELD, withSuggestion(message, suggestion));
    }

    // Only when the parser fails after the first line, e.g. not on an unterminated string literal
    if (message.startsWith("Encountered")
        && errorLine > 1
        && STATEMENT_START.matcher(statement.strip()).find()) {
      return new Hint(
          ErrorLabel.GENERIC,
          ErrorPrinter.shortenParserMessage(message)
              + "\nHint: is the ';' missing at the end of the previous statement?");
    }

    return new Hint(ErrorLabel.GENERIC, ErrorPrinter.shortenParserMessage(message));
  }

  private static String withSuggestion(String message, Optional<String> suggestion) {
    return suggestion
        .map(s -> "%s. Did you mean '%s'?".formatted(message.strip(), s))
        .orElse(message);
  }

  private static List<String> tableNames(CatalogManager catalogManager) {
    return catalogManager.listTables().stream().filter(ErrorHints::isUserFacing).toList();
  }

  private static List<String> columnNames(String statement, CatalogManager catalogManager) {
    var tables = SqlTableNameExtractor.findTableNames(statement);
    return tableNames(catalogManager).stream()
        .filter(table -> tables.stream().anyMatch(table::equalsIgnoreCase))
        .flatMap(
            table ->
                catalogManager
                    .getTable(
                        ObjectIdentifier.of(
                            catalogManager.getCurrentCatalog(),
                            catalogManager.getCurrentDatabase(),
                            table))
                    .stream())
        .flatMap(table -> table.getResolvedSchema().getColumnNames().stream())
        .filter(ErrorHints::isUserFacing)
        .distinct()
        .toList();
  }

  private static boolean isUserFacing(String name) {
    return !name.startsWith("_");
  }

  /** Returns the candidate closest to the given name, if it is close enough to be a typo. */
  static Optional<String> closestMatch(String name, Collection<String> candidates) {
    record Candidate(String name, int distance) {}

    var maxDistance = Math.max(2, name.length() / 3);
    return candidates.stream()
        .filter(candidate -> !candidate.equals(name))
        .map(candidate -> new Candidate(candidate, distance(name, candidate)))
        .filter(candidate -> candidate.distance() <= maxDistance)
        .min(Comparator.comparingInt(Candidate::distance))
        .map(Candidate::name);
  }

  /** Case-insensitive Levenshtein distance. */
  static int distance(String a, String b) {
    a = a.toLowerCase();
    b = b.toLowerCase();
    var prev = new int[b.length() + 1];
    var curr = new int[b.length() + 1];
    for (var j = 0; j <= b.length(); j++) {
      prev[j] = j;
    }
    for (var i = 1; i <= a.length(); i++) {
      curr[0] = i;
      for (var j = 1; j <= b.length(); j++) {
        var cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
        curr[j] = Math.min(Math.min(curr[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
      }
      var tmp = prev;
      prev = curr;
      curr = tmp;
    }
    return prev[b.length()];
  }
}
