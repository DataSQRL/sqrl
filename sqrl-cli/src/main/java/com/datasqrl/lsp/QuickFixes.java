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
import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.ScriptText.NamePart;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.eclipse.lsp4j.CodeAction;
import org.eclipse.lsp4j.CodeActionKind;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionItemKind;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.SymbolKind;
import org.eclipse.lsp4j.TextEdit;
import org.eclipse.lsp4j.WorkspaceEdit;

/**
 * Fixes for the diagnostics of the compiler in a script, which are found from its current text:
 *
 * <ul>
 *   <li>adding a missing {@code ;};
 *   <li>replacing an unknown table, column or hint with the closest known names;
 *   <li>adding the IMPORT of an unknown table or database that a script or table file defines;
 *   <li>selecting the sort columns of a table definition.
 * </ul>
 */
class QuickFixes {

  /** The most names that are suggested instead of an unknown name */
  private static final int MAX_NAMES = 3;

  private static final String MISSING_DELIMITER = "Missing ';' at the end of the statement";
  private static final String DID_YOU_MEAN = "(?:; did you mean '(?<suggestion>[^']+)'\\?)?";
  private static final Pattern UNKNOWN_OBJECT =
      Pattern.compile("Object '(?<name>[^']+)' not found(?: within '[^']*')?" + DID_YOU_MEAN);
  private static final Pattern UNKNOWN_COLUMN =
      Pattern.compile(
          "Column '(?<name>[^']+)' not found in (?:any table|table '[^']*')" + DID_YOU_MEAN);
  private static final Pattern UNKNOWN_HINT =
      Pattern.compile("Unrecognized hint: \\[(?<name>\\w+)]");

  /** An unknown hint of Flink in a query, see {@link DiagnosticConverter} */
  private static final Pattern UNKNOWN_QUERY_HINT =
      Pattern.compile("Unknown hint `(?<name>\\w+)`\\. Flink's hints .*", Pattern.DOTALL);

  /** An option of a hint of Flink in a query that names no input, see {@link QueryHints} */
  private static final Pattern UNKNOWN_HINT_INPUT =
      Pattern.compile(
          "Hint \\w+ names `(?<name>[^`]+)`, which is not a table or alias of this query.*"
              + " The query has: (?<names>.*)\\.",
          Pattern.DOTALL);

  /** The errors of SQRL statements about unknown tables, with the path of the table */
  private static final Pattern UNKNOWN_TABLE =
      Pattern.compile(
          "(?:Could not find (?:table|parent table for relationship|INSERT target table): "
              + "|Column expression must directly follow the definition of table \\[)"
              + "(?<path>[^]]+)]?");

  private static final Pattern MISSING_SORT_COLUMNS =
      Pattern.compile(
          "All sort columns must be part of the SELECT clause .*, missing: \\[(?<names>[^]]*)]");

  private static final Set<CompletionItemKind> TABLE_KINDS =
      Set.of(CompletionItemKind.Class, CompletionItemKind.Module);
  private static final Set<CompletionItemKind> COLUMN_KINDS = Set.of(CompletionItemKind.Field);

  private final Path file;
  private final String uri;
  private final ScriptText script;
  private final Function<CompletionContext, List<CompletionItem>> completion;
  private final Optional<ImportCandidates> imports;

  /**
   * @param file the script
   * @param uri the URI of the script
   * @param text the current content of the script
   * @param completion completes the script, which suggests the names that are known at a position
   * @param imports what the script can import, if the project was checked
   */
  QuickFixes(
      Path file,
      String uri,
      String text,
      Function<CompletionContext, List<CompletionItem>> completion,
      Optional<ImportCandidates> imports) {
    this.file = file;
    this.uri = uri;
    this.script = new ScriptText(text);
    this.completion = completion;
    this.imports = imports;
  }

  /** The fixes for a diagnostic of the compiler in the script. */
  List<CodeAction> fix(Diagnostic diagnostic) {
    var message = diagnostic.getMessage();
    var text = message.isLeft() ? message.getLeft() : message.getRight().getValue();
    if (MISSING_DELIMITER.equals(text)) {
      return addDelimiter(diagnostic);
    }
    Matcher matcher;
    if ((matcher = UNKNOWN_OBJECT.matcher(text)).matches()) {
      var name = matcher.group("name");
      var result = new ArrayList<>(replaceName(diagnostic, name, suggestion(matcher), TABLE_KINDS));
      result.addAll(addImport(diagnostic, name));
      return result;
    }
    if ((matcher = UNKNOWN_COLUMN.matcher(text)).matches()) {
      return replaceName(diagnostic, matcher.group("name"), suggestion(matcher), COLUMN_KINDS);
    }
    if ((matcher = UNKNOWN_TABLE.matcher(text)).matches()) {
      var path = matcher.group("path").split("\\.");
      var name = path[path.length - 1].replace("`", "");
      return replaceName(diagnostic, name, Optional.empty(), TABLE_KINDS);
    }
    if ((matcher = UNKNOWN_HINT.matcher(text)).matches()) {
      var name = matcher.group("name");
      return replaceClosest(diagnostic, name, closest(name, ScriptCompletion.hintNames()));
    }
    if ((matcher = UNKNOWN_QUERY_HINT.matcher(text)).matches()) {
      var names = new ArrayList<>(QueryHints.QUERY_HINTS);
      names.addAll(QueryHints.TABLE_HINTS);
      return replaceClosest(
          diagnostic, matcher.group("name"), closest(matcher.group("name"), names));
    }
    if ((matcher = UNKNOWN_HINT_INPUT.matcher(text)).matches()) {
      // Each input of the query, the closest first, since aliases are short
      var name = matcher.group("name");
      var inputs =
          Arrays.stream(matcher.group("names").split(", "))
              .map(input -> input.replace("`", ""))
              .sorted(Comparator.comparingInt(input -> distance(name, input)))
              .toList();
      return replaceClosest(diagnostic, name, inputs);
    }
    if ((matcher = MISSING_SORT_COLUMNS.matcher(text)).matches()) {
      return selectSortColumns(diagnostic, matcher.group("names"));
    }
    return List.of();
  }

  /** Replaces the name at a diagnostic with each of the given names. */
  private List<CodeAction> replaceClosest(Diagnostic diagnostic, String name, List<String> names) {
    return findName(diagnostic.getRange(), name)
        .map(found -> replaceWith(diagnostic, found.range(), name, names, false))
        .orElse(List.of());
  }

  private static Optional<String> suggestion(Matcher matcher) {
    return Optional.ofNullable(matcher.group("suggestion"));
  }

  /** Adds the {@code ;} after the code on the line of the diagnostic. */
  private List<CodeAction> addDelimiter(Diagnostic diagnostic) {
    var line = diagnostic.getRange().getEnd().getLine();
    if (line >= script.lineCount()) {
      return List.of();
    }
    var text = script.line(line);
    var end = codeEnd(text);
    if (end == 0 || text.charAt(end - 1) == ';') {
      return List.of();
    }
    var position = new Position(line, end);
    return List.of(
        action("Add the missing `;`", diagnostic, true, new TextEdit(range(position), ";")));
  }

  /** Where the code on a line ends, before trailing comments and whitespace. */
  private static int codeEnd(String line) {
    var end = 0;
    var quote = (char) 0;
    for (var i = 0; i < line.length(); i++) {
      var c = line.charAt(i);
      if (quote != 0) {
        quote = c == quote ? 0 : quote;
        end = i + 1;
      } else if (line.startsWith("--", i)) {
        break;
      } else if (line.startsWith("/*", i)) {
        var close = line.indexOf("*/", i + 2);
        if (close < 0) {
          break;
        }
        i = close + 1;
      } else if (c == '\'' || c == '"' || c == '`') {
        quote = c;
        end = i + 1;
      } else if (!Character.isWhitespace(c)) {
        end = i + 1;
      }
    }
    return end;
  }

  /**
   * Replaces an unknown name with the name that the compiler suggests, or else with the closest of
   * the known names. Tables are known by name, and columns are those that completion suggests at
   * the name.
   */
  private List<CodeAction> replaceName(
      Diagnostic diagnostic,
      String name,
      Optional<String> suggestion,
      Set<CompletionItemKind> kinds) {
    var found = findName(diagnostic.getRange(), name);
    if (found.isEmpty()) {
      return List.of();
    }
    var range = found.get().range();
    if (suggestion.isPresent()) {
      return replaceWith(diagnostic, range, name, List.of(suggestion.get()), true);
    }
    var context = CompletionContext.at(script.text(), offset(range.getStart()));
    if (kinds == TABLE_KINDS) {
      // The compiler cannot complete all tables in queries, e.g. in subqueries
      context =
          new CompletionContext(
              CompletionContext.Kind.TABLE,
              "",
              found.get().qualifier(),
              null,
              List.of(),
              null,
              0,
              context.testStatement());
    }
    var known =
        completion.apply(context).stream()
            .filter(item -> kinds.contains(item.getKind()))
            .map(CompletionItem::getLabel)
            .toList();
    return replaceWith(diagnostic, range, name, closest(name, known), true);
  }

  /**
   * Replaces a name with one of the names that are closest to it, which is preferred if it is
   * closer than the others.
   *
   * @param quote whether to quote names that are not SQL identifiers
   */
  private List<CodeAction> replaceWith(
      Diagnostic diagnostic, Range range, String name, List<String> names, boolean quote) {
    var preferred = names.size() < 2 || distance(name, names.get(0)) < distance(name, names.get(1));
    var result = new ArrayList<CodeAction>();
    for (var newName : names) {
      result.add(
          action(
              "Change to `" + newName + "`",
              diagnostic,
              preferred && result.isEmpty(),
              new TextEdit(
                  range,
                  quote && ScriptCompletion.needsQuotes(newName) ? "`" + newName + "`" : newName)));
    }
    return result;
  }

  /**
   * Adds the IMPORT of an unknown table, or of an unknown database whose tables are referenced as
   * {@code database.table}.
   */
  private List<CodeAction> addImport(Diagnostic diagnostic, String name) {
    var range = diagnostic.getRange();
    var parts = script.nameStartingAt(offset(range.getStart()));
    if (imports.isEmpty() || parts.isEmpty() || !parts.get(0).name().equals(name)) {
      return List.of();
    }
    var paths = parts.size() > 1 ? imports.get().scripts(name) : imports.get().tables(name);
    return paths.stream()
        .limit(MAX_NAMES)
        .map(
            path -> {
              var statement = "IMPORT " + path + ";";
              return action(
                  "Add `" + statement + "`",
                  diagnostic,
                  false,
                  importEdit(range.getStart().getLine(), statement));
            })
        .toList();
  }

  /**
   * Inserts an IMPORT statement after the last IMPORT statement before a line, or else before the
   * first statement.
   */
  private TextEdit importEdit(int line, String statement) {
    var statements = DocumentOutline.of(script.text(), file, List.of());
    var lastImport =
        statements.stream()
            .filter(s -> s.getKind() == SymbolKind.Module && s.getName().startsWith("IMPORT "))
            .filter(s -> s.getRange().getStart().getLine() < line)
            .reduce((first, second) -> second);
    if (lastImport.isPresent()) {
      var importLine = lastImport.get().getRange().getEnd().getLine();
      var end = new Position(importLine, script.line(importLine).length());
      return new TextEdit(range(end), "\n" + statement);
    }
    var firstLine =
        statements.stream().mapToInt(s -> s.getRange().getStart().getLine()).findFirst().orElse(0);
    return new TextEdit(range(new Position(Math.min(firstLine, line), 0)), statement + "\n");
  }

  /** Adds the sort columns that the SELECT clause of a table definition is missing. */
  private List<CodeAction> selectSortColumns(Diagnostic diagnostic, String missingNames) {
    var missing = Arrays.stream(missingNames.split(",")).map(String::strip).toList();
    return SortColumns.find(script, offset(diagnostic.getRange().getStart()), missing)
        .map(
            selection ->
                List.of(
                    action(
                        "Add `" + selection.columns() + "` to the SELECT clause",
                        diagnostic,
                        true,
                        new TextEdit(
                            range(position(selection.offset())), ", " + selection.columns()))))
        .orElse(List.of());
  }

  /**
   * The names that are closest to a name, ignoring case, if they are close enough to be a typo of
   * it.
   */
  static List<String> closest(String name, Collection<String> names) {
    var maxDistance = Math.max(1, Math.min(3, name.length() / 3));
    return names.stream()
        .distinct()
        .filter(candidate -> !candidate.equals(name))
        .map(candidate -> Map.entry(candidate, distance(name, candidate)))
        .filter(entry -> entry.getValue() <= maxDistance)
        .sorted(
            Map.Entry.<String, Integer>comparingByValue().thenComparing(Map.Entry.comparingByKey()))
        .limit(MAX_NAMES)
        .map(Map.Entry::getKey)
        .toList();
  }

  /**
   * The number of characters to insert, delete, replace or swap with the next one to turn one name
   * into another, ignoring case.
   */
  static int distance(String from, String to) {
    var a = from.toLowerCase(Locale.ROOT);
    var b = to.toLowerCase(Locale.ROOT);
    var d = new int[a.length() + 1][b.length() + 1];
    for (var i = 0; i <= a.length(); i++) {
      d[i][0] = i;
    }
    for (var j = 0; j <= b.length(); j++) {
      d[0][j] = j;
    }
    for (var i = 1; i <= a.length(); i++) {
      for (var j = 1; j <= b.length(); j++) {
        var cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
        d[i][j] = Math.min(Math.min(d[i - 1][j], d[i][j - 1]) + 1, d[i - 1][j - 1] + cost);
        if (i > 1
            && j > 1
            && a.charAt(i - 1) == b.charAt(j - 2)
            && a.charAt(i - 2) == b.charAt(j - 1)) {
          d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + 1);
        }
      }
    }
    return d[a.length()][b.length()];
  }

  /**
   * A name in the text.
   *
   * @param range where it is
   * @param qualifier the names before it in its qualified name, e.g. {@code [sources]}
   */
  private record Name(Range range, List<String> qualifier) {}

  /**
   * Finds the name that a diagnostic was reported for in the current text: in the qualified name
   * that starts at the diagnostic, e.g. {@code Ordrs} in {@code sources.Ordrs}, or else in the
   * statement, for errors that are reported at the start of their statement.
   */
  private Optional<Name> findName(Range range, String name) {
    if (range.getStart().getLine() != range.getEnd().getLine()) {
      return Optional.empty();
    }
    var start = offset(range.getStart());
    var parts = script.nameStartingAt(start);
    for (var i = parts.size() - 1; i >= 0; i--) {
      if (parts.get(i).name().equals(name)) {
        var qualifier = parts.subList(0, i).stream().map(NamePart::name).toList();
        return Optional.of(new Name(range(parts.get(i)), qualifier));
      }
    }
    return script
        .find(name, start)
        .filter(found -> found.size() == 1 && found.get(0).name().equals(name))
        .map(found -> new Name(range(found.get(0)), List.of()));
  }

  private Range range(NamePart part) {
    return new Range(position(part.start()), position(part.end()));
  }

  private CodeAction action(String title, Diagnostic diagnostic, boolean preferred, TextEdit edit) {
    var action = new CodeAction(title);
    action.setKind(CodeActionKind.QuickFix);
    action.setDiagnostics(List.of(diagnostic));
    if (preferred) {
      action.setIsPreferred(true);
    }
    action.setEdit(new WorkspaceEdit(Map.of(uri, List.of(edit))));
    return action;
  }

  private int offset(Position position) {
    return script.offset(position.getLine(), position.getCharacter());
  }

  private Position position(int offset) {
    return new Position(script.lineOf(offset), script.characterOf(offset));
  }

  private static Range range(Position position) {
    return new Range(position, position);
  }
}
