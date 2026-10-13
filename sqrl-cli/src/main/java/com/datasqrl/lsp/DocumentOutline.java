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
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.planner.parser.FlinkSQLStatement;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlExportStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;
import org.eclipse.lsp4j.DocumentSymbol;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.eclipse.lsp4j.SymbolKind;

/**
 * The outline of a script: its statements and the tables, functions and relationships they define.
 * It is parsed from the current content of the script, so it is up to date while the script has
 * errors. The columns of tables come from the last check.
 */
class DocumentOutline {

  /** The name of a CREATE TABLE, VIEW or FUNCTION or INSERT statement after leading comments */
  private static final Pattern FLINK_DEFINITION =
      Pattern.compile(
          "^(?:\\s++|/\\*.*?\\*/|--[^\\n]*+)*+"
              + "(?:CREATE\\s+(?:TEMPORARY\\s+)?(?:SYSTEM\\s+)?(TABLE|VIEW|FUNCTION)\\s+"
              + "(?:IF\\s+NOT\\s+EXISTS\\s+)?|INSERT\\s+(?:INTO|OVERWRITE)\\s+)([\\w.`$]+)",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private final SqrlStatementParser parser = new SqrlStatementParser();
  private final ScriptText script;
  private final Path file;
  private final List<SymbolIndex> indexes;

  private DocumentOutline(String text, Path file, List<SymbolIndex> indexes) {
    this.script = new ScriptText(text);
    this.file = file;
    this.indexes = indexes;
  }

  static List<DocumentSymbol> of(String text, Path file, List<SymbolIndex> indexes) {
    return new DocumentOutline(text, file, indexes).build();
  }

  private List<DocumentSymbol> build() {
    List<ParsedStatement> statements;
    try {
      var errors = ErrorCollector.root().withScript(Optional.of(file), script.text());
      statements = parser.parseScript(script.text(), errors).statements();
    } catch (RuntimeException e) {
      return List.of(); // e.g. an empty script
    }
    var result = new ArrayList<DocumentSymbol>();
    for (var statement : statements) {
      try {
        addStatement(statement, result);
      } catch (RuntimeException e) {
        // Leave out statements that cannot be outlined
      }
    }
    return result;
  }

  private void addStatement(ParsedStatement statement, List<DocumentSymbol> result) {
    var startLine = statement.statement().getFileLocation().line() - 1;
    var endLine =
        startLine + (int) statement.source().strip().chars().filter(c -> c == '\n').count();
    var range =
        new Range(new Position(startLine, 0), new Position(endLine, script.line(endLine).length()));
    var sql = statement.statement().get();
    if (sql instanceof SqrlImportStatement importStatement) {
      var path = importStatement.getPackageIdentifier().get().getDisplay();
      var symbol = symbol("IMPORT " + path, SymbolKind.Module, range, path);
      var alias = importStatement.getAlias();
      if (alias.isPresent()) {
        symbol.setDetail("as " + alias.get().getDisplay());
      }
      result.add(symbol);
    } else if (sql instanceof SqrlExportStatement export) {
      var table = export.getTableIdentifier().get().getDisplay();
      var symbol = symbol("EXPORT " + table, SymbolKind.Event, range, table);
      symbol.setDetail("to " + export.getPackageIdentifier().get().getDisplay());
      result.add(symbol);
    } else if (sql instanceof SqrlDefinition definition) {
      addDefinition(definition, range, result);
    } else if (sql instanceof SqrlCreateTableStatement createTable) {
      addFlinkDefinition(createTable.toSql(), range, result);
    } else if (sql instanceof FlinkSQLStatement flinkStatement) {
      addFlinkDefinition(flinkStatement.sql().get(), range, result);
    }
  }

  private void addDefinition(SqrlDefinition definition, Range range, List<DocumentSymbol> result) {
    var path = definition.getPath();
    var name = path.getDisplay();
    if (definition instanceof SqrlAddColumnStatement || definition.isRelationship()) {
      var kind =
          definition instanceof SqrlAddColumnStatement ? SymbolKind.Field : SymbolKind.Property;
      var parentName = path.popLast().getDisplay();
      var parent =
          result.stream().filter(s -> s.getName().equalsIgnoreCase(parentName)).reduce((a, b) -> b);
      var symbol =
          symbol(parent.isPresent() ? path.getLast().getDisplay() : name, kind, range, name);
      if (parent.isPresent()) {
        parent.get().getChildren().add(symbol);
        if (parent.get() == result.get(result.size() - 1)) {
          // Keep the range of the parent around children that directly follow it
          parent.get().getRange().setEnd(range.getEnd());
        }
      } else {
        result.add(symbol);
      }
      return;
    }
    if (definition instanceof SqrlTableFunctionStatement function) {
      var symbol = symbol(name, SymbolKind.Function, range, name);
      Optional.ofNullable(function.getSignature().get())
          .ifPresent(signature -> symbol.setDetail("(" + signature.strip() + ")"));
      result.add(symbol);
      return;
    }
    var symbol = symbol(name, SymbolKind.Class, range, name);
    var details = new ArrayList<String>();
    var indexed = indexedTable(name);
    indexed.ifPresent(table -> details.add(tableType(table.tableType())));
    if (definition.getComments().containsHintByName("test"::equalsIgnoreCase)) {
      details.add("test");
    }
    symbol.setDetail(String.join(" · ", details));
    indexed.ifPresent(table -> addColumns(symbol, table));
    result.add(symbol);
  }

  private void addFlinkDefinition(String sql, Range range, List<DocumentSymbol> result) {
    var matcher = FLINK_DEFINITION.matcher(sql);
    if (!matcher.find()) {
      return;
    }
    var name = matcher.group(2).replace("`", "");
    var type = matcher.group(1);
    if (type == null) {
      result.add(symbol("INSERT INTO " + name, SymbolKind.Event, range, name));
    } else if (type.equalsIgnoreCase("FUNCTION")) {
      result.add(symbol(name, SymbolKind.Function, range, name));
    } else {
      var symbol =
          symbol(
              name,
              type.equalsIgnoreCase("TABLE") ? SymbolKind.Struct : SymbolKind.Class,
              range,
              name);
      indexedTable(name)
          .ifPresent(
              table -> {
                symbol.setDetail(tableType(table.tableType()));
                addColumns(symbol, table);
              });
      result.add(symbol);
    }
  }

  /** Adds the columns of the last check, except those that are added by separate statements. */
  private static void addColumns(DocumentSymbol symbol, Symbol table) {
    var added = new HashSet<String>();
    table.columnLocations().keySet().forEach(c -> added.add(c.toLowerCase(Locale.ROOT)));
    for (var column : table.columns()) {
      if (!added.contains(column.name().toLowerCase(Locale.ROOT))) {
        var child =
            new DocumentSymbol(
                column.name(),
                SymbolKind.Field,
                symbol.getSelectionRange(),
                symbol.getSelectionRange());
        child.setDetail(column.type());
        symbol.getChildren().add(child);
      }
    }
  }

  /** The table with a name that is defined in this file, from the last check. */
  private Optional<Symbol> indexedTable(String name) {
    return indexes.stream()
        .flatMap(index -> index.symbolsNamed(name).stream())
        .filter(s -> s.kind() == Kind.TABLE && s.location().file().equals(file))
        .findFirst();
  }

  /** A symbol whose selection range is the name in the statement, or its first line. */
  private DocumentSymbol symbol(String name, SymbolKind kind, Range range, String nameInText) {
    var start = range.getStart().getLine();
    var selection =
        script
            .find(nameInText, script.offset(start, 0))
            .map(
                parts -> {
                  var from = parts.get(0).start();
                  var to = parts.get(parts.size() - 1).end();
                  return new Range(
                      new Position(script.lineOf(from), script.characterOf(from)),
                      new Position(script.lineOf(to), script.characterOf(to)));
                })
            .filter(r -> r.getEnd().getLine() <= range.getEnd().getLine())
            .orElseGet(
                () ->
                    new Range(
                        new Position(start, 0), new Position(start, script.line(start).length())));
    var symbol = new DocumentSymbol(name, kind, copy(range), selection);
    symbol.setChildren(new ArrayList<>());
    return symbol;
  }

  private static Range copy(Range range) {
    return new Range(
        new Position(range.getStart().getLine(), range.getStart().getCharacter()),
        new Position(range.getEnd().getLine(), range.getEnd().getCharacter()));
  }

  private static String tableType(String tableType) {
    return tableType.toLowerCase(Locale.ROOT).replace('_', ' ');
  }
}
