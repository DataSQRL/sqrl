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
package com.datasqrl.lsp;

import com.datasqrl.compile.check.QueryHints;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.lsp.CompletionContext.Kind;
import com.datasqrl.plan.global.IndexType;
import com.datasqrl.planner.hint.EngineHint;
import com.datasqrl.planner.hint.IndexHint;
import com.datasqrl.planner.hint.MaintenanceHint;
import com.datasqrl.planner.hint.MutationInsertHint;
import com.datasqrl.planner.hint.PartitionKeyHint;
import com.datasqrl.planner.hint.PrimaryKeyHint;
import com.datasqrl.planner.hint.QueryByAllHint;
import com.datasqrl.planner.hint.QueryByAnyHint;
import com.datasqrl.planner.hint.RowCountHint;
import com.datasqrl.planner.hint.TestHint;
import com.datasqrl.planner.hint.VectorDimensionHint;
import com.datasqrl.server.MutationInsertType;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * What can be completed in a hint comment: the names of hints, and their arguments, which are the
 * columns of the table that the hint is for, e.g. in {@code query_by_all(}, or the values that the
 * hint accepts, e.g. the types of indexes.
 */
final class HintArguments {

  private static final int FLAGS = Pattern.CASE_INSENSITIVE | Pattern.DOTALL;
  private static final Pattern IMPORT =
      Pattern.compile("^\\s*IMPORT\\s+([\\w.`$-]+)(?:\\s+AS\\s+([\\w`]+))?", FLAGS);
  private static final Pattern CREATE_TABLE =
      Pattern.compile(
          "^\\s*CREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([\\w.`]+)",
          FLAGS);

  /** The end of the query before a hint of Flink for the query, e.g. a join hint */
  private static final Pattern AFTER_SELECT = Pattern.compile("\\bSELECT\\s*$", FLAGS);

  /** A table in a FROM clause of a query, with its alias, e.g. {@code JOIN Orders AS o} */
  private static final Pattern FROM_TABLE =
      Pattern.compile("\\b(?:FROM|JOIN)\\s+([\\w.`$]+)(?:\\s+(?:AS\\s+)?([\\w`$]+))?", FLAGS);

  /** Keywords that may follow a table in a FROM clause instead of an alias */
  private static final Set<String> NOT_ALIASES =
      Set.of(
          ("ON WHERE JOIN INNER LEFT RIGHT FULL CROSS OUTER NATURAL USING GROUP ORDER HAVING LIMIT"
                  + " UNION EXCEPT INTERSECT FOR WINDOW LATERAL TABLE")
              .split(" "));

  private static volatile List<String> engineNames;

  private HintArguments() {}

  /**
   * The context in a hint comment: the name of a hint, or one of its arguments.
   *
   * @param beforeCursor the hint comment from after {@code /*+} up to the position
   * @param statement the statement after the hint comment, without comments
   */
  static CompletionContext at(String beforeCursor, String statement) {
    var open = openParenthesis(beforeCursor);
    var prefix = CompletionContext.trailingName(beforeCursor);
    if (open < 0) {
      return context(Kind.HINT, prefix, null, List.of());
    }
    var hint =
        CompletionContext.trailingName(beforeCursor.substring(0, open).stripTrailing())
            .toLowerCase(Locale.ROOT);
    var argument = (int) beforeCursor.substring(open + 1).chars().filter(c -> c == ',').count();
    var values = values(hint, argument);
    if (!values.isEmpty()) {
      return context(Kind.KEYWORD, prefix, null, values);
    }
    if (isColumn(hint, argument)) {
      return hintedTable(statement)
          .map(table -> context(Kind.HINT_COLUMN, prefix, table, List.of()))
          .orElse(CompletionContext.NONE);
    }
    return CompletionContext.NONE;
  }

  /**
   * The context in a hint of Flink in a query: the name of a hint, or an input of the query that a
   * join hint names, e.g. after {@code SELECT /*+ BROADCAST(}.
   *
   * @param beforeCursor the hint comment from after {@code /*+} up to the position
   * @param queryBefore the statement before the hint comment, without comments
   * @param queryAfter the rest of the statement after the hint comment, without comments
   */
  static CompletionContext inQuery(String beforeCursor, String queryBefore, String queryAfter) {
    var open = openParenthesis(beforeCursor);
    var prefix = CompletionContext.trailingName(beforeCursor);
    if (open < 0) {
      var names =
          AFTER_SELECT.matcher(queryBefore).find()
              ? QueryHints.QUERY_HINTS
              : QueryHints.TABLE_HINTS;
      return context(Kind.KEYWORD, prefix, null, names);
    }
    var hint = CompletionContext.trailingName(beforeCursor.substring(0, open).stripTrailing());
    if (!QueryHints.isJoinHint(hint)) {
      return CompletionContext.NONE;
    }
    var inputs = new LinkedHashSet<String>();
    var matcher = FROM_TABLE.matcher(queryAfter);
    while (matcher.find()) {
      var table = matcher.group(1).replace("`", "");
      var alias = matcher.group(2) == null ? "" : matcher.group(2).replace("`", "");
      if (NOT_ALIASES.contains(table.toUpperCase(Locale.ROOT))) {
        continue; // e.g. a table function, TABLE(...)
      }
      inputs.add(
          alias.isEmpty() || NOT_ALIASES.contains(alias.toUpperCase(Locale.ROOT))
              ? table.substring(table.lastIndexOf('.') + 1)
              : alias);
    }
    return context(Kind.NAME, prefix, null, List.copyOf(inputs));
  }

  /** The offset of the open parenthesis of the hint at the end, or -1 if at the name of a hint. */
  private static int openParenthesis(String beforeCursor) {
    var open = -1;
    var depth = 0;
    for (var i = 0; i < beforeCursor.length(); i++) {
      var c = beforeCursor.charAt(i);
      if (c == '(' && depth++ == 0) {
        open = i;
      } else if (c == ')' && depth > 0) {
        depth--;
      }
    }
    return depth == 0 ? -1 : open;
  }

  private static CompletionContext context(
      Kind kind, String prefix, String table, List<String> names) {
    return new CompletionContext(kind, prefix, List.of(), table, names, null, 0, false);
  }

  /** The table that a statement defines, which a hint before it is for. */
  private static Optional<String> hintedTable(String statement) {
    Matcher matcher;
    if ((matcher = CompletionContext.DEFINITION.matcher(statement)).find()) {
      return Optional.of(matcher.group("name").replace("`", ""));
    } else if ((matcher = IMPORT.matcher(statement)).find()) {
      var path = matcher.group(1).replace("`", "");
      return Optional.of(
          matcher.group(2) != null
              ? matcher.group(2).replace("`", "")
              : path.substring(path.lastIndexOf('.') + 1));
    } else if ((matcher = CREATE_TABLE.matcher(statement)).find()) {
      return Optional.of(matcher.group(1).replace("`", ""));
    }
    return Optional.empty();
  }

  private static boolean isColumn(String hint, int argument) {
    return switch (hint) {
      case PrimaryKeyHint.HINT_NAME,
          PartitionKeyHint.HINT_NAME,
          QueryByAllHint.HINT_NAME,
          QueryByAnyHint.HINT_NAME,
          RowCountHint.HINT_NAME ->
          true;
      // The type of the index comes first
      case IndexHint.HINT_NAME -> argument > 0;
      // The number of dimensions comes last
      case VectorDimensionHint.HINT_NAME -> argument == 0;
      default -> false;
    };
  }

  private static List<String> values(String hint, int argument) {
    return switch (hint) {
      case IndexHint.HINT_NAME -> argument == 0 ? names(IndexType.values()) : List.of();
      case MaintenanceHint.HINT_NAME -> names(MaintenanceHint.MaintenanceType.values());
      case MutationInsertHint.HINT_NAME -> names(MutationInsertType.values());
      case TestHint.HINT_NAME -> List.of("no_rows");
      case EngineHint.HINT_NAME, EngineHint.OLD_HINT_NAME -> engineNames();
      default -> List.of();
    };
  }

  private static List<String> names(Enum<?>[] values) {
    return Stream.of(values).map(value -> value.name().toLowerCase(Locale.ROOT)).toList();
  }

  /** The names of the engines that the CLI supports, e.g. {@code postgres}. */
  private static List<String> engineNames() {
    if (engineNames == null) {
      var names = new TreeSet<String>();
      ServiceLoaderDiscovery.getAll(EngineFactory.class)
          .forEach(factory -> names.add(factory.getEngineName()));
      engineNames = List.copyOf(names);
    }
    return engineNames;
  }
}
