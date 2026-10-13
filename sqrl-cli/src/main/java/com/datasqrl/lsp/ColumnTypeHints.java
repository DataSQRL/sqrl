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

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Column;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.eclipse.lsp4j.InlayHint;
import org.eclipse.lsp4j.InlayHintKind;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.jsonrpc.messages.Either;

/**
 * Shows the types of the columns that a script computes: of the columns of a query that are named
 * with {@code AS}, e.g. {@code SUM(amount) AS total}, and of columns that are added to a table,
 * e.g. {@code Orders.total := ...}. The statements are parsed from the current content of the
 * script, and the types come from the last check.
 */
class ColumnTypeHints {

  /** Long types, e.g. of nested rows, are cut off in the hint and shown in full in its tooltip */
  private static final int MAX_TYPE = 40;

  private static final Pattern WORD = Pattern.compile("[A-Za-z_][\\w$]*");

  private static final Pattern SELECT_MODIFIER =
      Pattern.compile("\\s+(?:DISTINCT|ALL)\\b", Pattern.CASE_INSENSITIVE);

  /** The name of a column at the end of a select item */
  private static final Pattern ALIAS =
      Pattern.compile("\\bAS\\s+(`[^`]+`|[A-Za-z_][\\w$]*)\\s*$", Pattern.CASE_INSENSITIVE);

  private final SqrlStatementParser parser = new SqrlStatementParser();
  private final Path file;
  private final ScriptText script;
  private final List<SymbolIndex> indexes;
  private final List<InlayHint> hints = new ArrayList<>();

  private ColumnTypeHints(Path file, String text, List<SymbolIndex> indexes) {
    this.file = file;
    this.script = new ScriptText(text);
    this.indexes = indexes;
  }

  /** The hints of the lines in a range. */
  static List<InlayHint> of(Path file, String text, List<SymbolIndex> indexes, Range range) {
    var hints = new ColumnTypeHints(file, text, indexes);
    hints.build();
    return hints.hints.stream()
        .filter(
            hint ->
                hint.getPosition().getLine() >= range.getStart().getLine()
                    && hint.getPosition().getLine() <= range.getEnd().getLine())
        .toList();
  }

  private void build() {
    if (indexes.isEmpty()) {
      return;
    }
    List<ParsedStatement> statements;
    try {
      var errors = ErrorCollector.root().withScript(Optional.of(file), script.text());
      statements = parser.parseScript(script.text(), errors).statements();
    } catch (RuntimeException e) {
      return; // e.g. an empty script
    }
    for (var statement : statements) {
      try {
        if (statement.statement().get() instanceof SqrlDefinition definition) {
          add(definition, statement.statement().getFileLocation());
        }
      } catch (RuntimeException e) {
        // Leave out statements that cannot be parsed
      }
    }
  }

  /**
   * Adds the hints of a definition.
   *
   * @param statement where the statement starts, which the locations in it are relative to
   */
  private void add(SqrlDefinition definition, FileLocation statement) {
    var path = definition.getPath();
    if (definition instanceof SqrlAddColumnStatement) {
      // The type after the name of the added column
      var table = definedSymbol(path.popLast().getDisplay(), Kind.TABLE);
      var column = table.flatMap(t -> t.column(path.getLast().getDisplay()));
      var name =
          script.find(
              path.getDisplay(),
              offset(statement.add(definition.getTableName().getFileLocation())));
      if (column.isPresent() && name.isPresent()) {
        var parts = name.get();
        add(parts.get(parts.size() - 1).end(), column.get());
      }
      return;
    }
    var symbol = definedSymbol(path.getDisplay(), null);
    if (symbol.isEmpty()) {
      return;
    }
    var body = definition.getDefinitionBody();
    var start = offset(statement.add(body.getFileLocation()));
    for (var item : selectItems(body.get())) {
      var alias = ALIAS.matcher(item.text());
      if (alias.find()) {
        var columnName = alias.group(1).replace("`", "");
        symbol
            .get()
            .column(columnName)
            .ifPresent(column -> add(start + item.start() + alias.end(1), column));
      }
    }
  }

  private void add(int offset, Column column) {
    var type = column.type();
    var label = ": " + (type.length() > MAX_TYPE ? type.substring(0, MAX_TYPE - 1) + "…" : type);
    var hint =
        new InlayHint(
            new Position(script.lineOf(offset), script.characterOf(offset)), Either.forLeft(label));
    hint.setKind(InlayHintKind.Type);
    if (!label.endsWith(type)) {
      hint.setTooltip(type);
    }
    hints.add(hint);
  }

  /** The table, function or relationship with a name that this script defines. */
  private Optional<Symbol> definedSymbol(String name, Kind kind) {
    return indexes.stream()
        .flatMap(index -> index.symbolsNamed(name).stream())
        .filter(s -> s.location().file().equals(file) && (kind == null || s.kind() == kind))
        .findFirst();
  }

  /** The offset of a location of the parser, whose lines and columns are 1-based. */
  private int offset(FileLocation location) {
    return script.offset(location.line() - 1, location.offset() - 1);
  }

  /** An item of a select list and where it starts in the query. */
  record SelectItem(String text, int start) {}

  /**
   * The items of the select list of a query, i.e. of its first SELECT that is not in parentheses,
   * without comments and with string literals blanked out.
   */
  static List<SelectItem> selectItems(String query) {
    var text = blankCommentsAndStrings(query);
    var items = new ArrayList<SelectItem>();
    var depth = 0;
    var itemStart = -1;
    var words = WORD.matcher(text);
    var i = 0;
    while (i < text.length()) {
      var c = text.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && c == ',' && itemStart >= 0) {
        items.add(new SelectItem(text.substring(itemStart, i), itemStart));
        itemStart = i + 1;
      } else if (depth == 0 && c == ';') {
        break;
      } else if (depth == 0 && words.region(i, text.length()).lookingAt()) {
        var word = words.group();
        if (itemStart < 0 && word.equalsIgnoreCase("SELECT")) {
          var modifier = SELECT_MODIFIER.matcher(text).region(words.end(), text.length());
          itemStart = modifier.lookingAt() ? modifier.end() : words.end();
        } else if (itemStart >= 0 && word.equalsIgnoreCase("FROM")) {
          items.add(new SelectItem(text.substring(itemStart, i), itemStart));
          return items;
        }
        i = words.end();
        continue;
      }
      i++;
    }
    if (itemStart >= 0 && itemStart < i) {
      items.add(new SelectItem(text.substring(itemStart, i), itemStart));
    }
    return items;
  }

  /** Replaces comments and the content of string literals with spaces, keeping line breaks. */
  private static String blankCommentsAndStrings(String query) {
    var text = new StringBuilder(query);
    var i = 0;
    while (i < text.length()) {
      var end = i;
      if (query.startsWith("--", i)) {
        end = query.indexOf('\n', i);
      } else if (query.startsWith("/*", i)) {
        end = query.indexOf("*/", i + 2);
        end = end < 0 ? -1 : end + 2;
      } else if (query.charAt(i) == '\'') {
        end = i + 1;
        while (end < query.length() && (query.charAt(end) != '\'' || query.startsWith("''", end))) {
          end += query.startsWith("''", end) ? 2 : 1;
        }
        end = end < query.length() ? end + 1 : -1;
      }
      if (end < 0) {
        end = query.length();
      }
      if (end > i) {
        for (var j = i; j < end; j++) {
          if (text.charAt(j) != '\n') {
            text.setCharAt(j, ' ');
          }
        }
        i = end;
      } else {
        i++;
      }
    }
    return text.toString();
  }
}
