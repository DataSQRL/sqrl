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

import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlImportStatement;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

/**
 * Tracks the tables (and catalogs) that failed to plan, so that statements referencing them can be
 * skipped instead of reporting knock-on errors (e.g. "Object 'X' not found") for every use of a
 * failed table.
 *
 * <p>References are detected by name in the statement text, ignoring comments and string literals.
 * Tables of imported scripts are tracked with their namespace (e.g. {@code sources.Orders}), so
 * that they are not confused with local tables of the same name. Unquoted SQL reserved words are
 * ignored, so that a failed table named like a keyword (e.g. {@code Order}) does not match every
 * {@code ORDER BY}.
 *
 * <p>A USE statement that failed or was skipped sets no catalog or database, so the statements
 * after it would run in another one than intended. They are all skipped then, see {@link
 * #isContextFailed()}.
 */
class FailedTables {

  private static final Pattern COMMENT_OR_STRING =
      Pattern.compile("/\\*.*?\\*/|'(?:[^']|'')*'", Pattern.DOTALL);
  private static final String IDENTIFIER_REGEX = "(?:`[^`]+`|[A-Za-z_][\\w$]*)";
  private static final Pattern IDENTIFIER = Pattern.compile(IDENTIFIER_REGEX);

  /** A name with optional qualifiers, e.g. {@code sources.Orders} or {@code Customers.orders}. */
  private static final Pattern QUALIFIED_NAME =
      Pattern.compile(IDENTIFIER_REGEX + "(?:\\s*\\.\\s*" + IDENTIFIER_REGEX + ")*");

  private static final Pattern CREATE_NAME =
      Pattern.compile(
          "^\\s*CREATE\\s+(?:TEMPORARY\\s+)?(?:TABLE|VIEW|CATALOG)\\s+"
              + "(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([\\w.]+)",
          Pattern.CASE_INSENSITIVE);

  /** A USE statement, which sets the catalog, database or modules of the statements after it */
  private static final Pattern USE = Pattern.compile("^\\s*USE\\s", Pattern.CASE_INSENSITIVE);

  private static final SqlAbstractParserImpl.Metadata SQL_METADATA =
      FlinkSqlParserImpl.FACTORY.getParser(new StringReader("")).getMetadata();

  private final Set<String> names = new HashSet<>();

  /** Whether a USE statement failed or was skipped */
  private boolean contextFailed = false;

  int size() {
    return names.size();
  }

  /**
   * Whether a USE statement failed or was skipped, so that the errors of the statements after it
   * would be knock-on errors of running in another catalog or database than intended.
   */
  boolean isContextFailed() {
    return contextFailed;
  }

  /**
   * Restores whether the context failed, e.g. after a script that is imported with a context of its
   * own.
   */
  void restoreContextFailed(boolean failed) {
    contextFailed = failed;
  }

  /** Whether the statement references a table that failed to plan. */
  boolean isReferencedBy(String statementSource) {
    if (names.isEmpty()) {
      return false;
    }
    var matcher =
        QUALIFIED_NAME.matcher(COMMENT_OR_STRING.matcher(statementSource).replaceAll(" "));
    while (matcher.find()) {
      var segments = new ArrayList<String>();
      var segmentMatcher = IDENTIFIER.matcher(matcher.group());
      while (segmentMatcher.find()) {
        segments.add(segmentMatcher.group());
      }
      var first = segments.get(0);
      if (segments.size() == 1
          && !first.startsWith("`")
          && SQL_METADATA.isReservedWord(first.toUpperCase(Locale.ROOT))) {
        continue;
      }
      var unquoted = segments.stream().map(FailedTables::unquote).toList();
      // The whole name, or its first part for a table that qualifies a column or relationship
      if (names.contains(normalize(String.join(".", unquoted)))
          || names.contains(normalize(unquoted.get(0)))) {
        return true;
      }
    }
    return false;
  }

  private static String unquote(String identifier) {
    return identifier.startsWith("`")
        ? identifier.substring(1, identifier.length() - 1)
        : identifier;
  }

  /**
   * Records the table defined by a statement that failed to plan.
   *
   * @param statement the statement that failed
   * @param statementSource the source text of the statement
   * @param recordedNestedFailures whether failures were recorded while planning the statement, e.g.
   *     in an imported script. Then only those tables failed, not the whole import.
   * @param namespace the namespace of the script's tables (e.g. {@code sources} for {@code IMPORT
   *     package.sources}), or null for the main script
   */
  void recordFailure(
      SQLStatement statement,
      String statementSource,
      boolean recordedNestedFailures,
      String namespace) {
    var withoutComments = COMMENT_OR_STRING.matcher(statementSource).replaceAll(" ");
    if (USE.matcher(withoutComments).find()) {
      contextFailed = true;
    }
    if (statement instanceof SqrlDefinition definition) {
      if (definition.isTable()) {
        add(namespace, definition.getPath().getDisplay());
      }
    } else if (statement instanceof SqrlImportStatement importStatement) {
      if (!recordedNestedFailures) {
        var name =
            importStatement.getAlias().isPresent()
                ? importStatement.getAlias().get()
                : importStatement.getPackageIdentifier().get();
        add(namespace, lastSegment(name.getDisplay()));
      }
    } else {
      var matcher = CREATE_NAME.matcher(withoutComments);
      if (matcher.find()) {
        add(namespace, lastSegment(matcher.group(1)));
      }
    }
  }

  /** Records the table defined by a statement that was skipped because it references one. */
  void recordSkipped(SQLStatement statement, String statementSource, String namespace) {
    recordFailure(statement, statementSource, false, namespace);
  }

  private void add(String namespace, String name) {
    if (!name.isBlank() && !name.equals("*")) {
      names.add(normalize(namespace == null ? name : namespace + "." + name));
    }
  }

  private static String lastSegment(String path) {
    return path.substring(path.lastIndexOf('.') + 1);
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
