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

import com.datasqrl.compile.check.SymbolIndex.Column;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.lsp.SymbolLookup.ColumnTarget;
import com.datasqrl.lsp.SymbolLookup.DatabaseTarget;
import com.datasqrl.lsp.SymbolLookup.ImportTarget;
import com.datasqrl.lsp.SymbolLookup.ImportedTableTarget;
import com.datasqrl.lsp.SymbolLookup.SymbolTarget;
import com.datasqrl.lsp.SymbolLookup.Target;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

/** Renders the hover text of what a position refers to, as Markdown. */
@UtilityClass
class HoverContent {

  private static final int MAX_COLUMNS = 50;

  static Optional<String> render(Target target) {
    if (target instanceof SymbolTarget symbol) {
      return Optional.of(symbol(symbol.symbol()));
    } else if (target instanceof ColumnTarget column) {
      return Optional.of(column(column));
    } else if (target instanceof DatabaseTarget database) {
      return Optional.of(database(database));
    } else if (target instanceof ImportTarget imports && !imports.tables().isEmpty()) {
      return Optional.of(imports(imports.tables()));
    } else if (target instanceof ImportedTableTarget imported && !imported.tables().isEmpty()) {
      return Optional.of(imports(imported.tables()));
    }
    return Optional.empty();
  }

  /** The table that an IMPORT statement imports, or the names of the tables, e.g. of `.*`. */
  private static String imports(List<Symbol> tables) {
    if (tables.size() == 1) {
      return symbol(tables.get(0));
    }
    return "Imports "
        + tables.stream()
            .limit(MAX_COLUMNS)
            .map(table -> "`" + table.name() + "`")
            .collect(Collectors.joining(", "))
        + (tables.size() > MAX_COLUMNS ? ", …" : "")
        + " from `"
        + tables.get(0).location().file().getFileName()
        + "`";
  }

  private static String database(DatabaseTarget database) {
    var tables = database.tables();
    var text = new StringBuilder("```sql\nDATABASE ").append(database.name()).append("\n```\n\n");
    text.append("Imported from ")
        .append(
            database.scripts().stream()
                .map(script -> "`" + script.file().getFileName() + "`")
                .distinct()
                .collect(Collectors.joining(", ")));
    text.append(" · ")
        .append(
            tables.stream()
                .limit(MAX_COLUMNS)
                .map(table -> "`" + table.name() + "`")
                .collect(Collectors.joining(", ")));
    if (tables.size() > MAX_COLUMNS) {
      text.append(" and ").append(tables.size() - MAX_COLUMNS).append(" more");
    }
    return text.toString();
  }

  private static String symbol(Symbol symbol) {
    var text = new StringBuilder();
    text.append("```sql\n").append(symbol.name());
    if (!symbol.parameters().isEmpty()) {
      text.append(
          symbol.parameters().stream()
              .map(p -> p.name() + " " + p.type())
              .collect(Collectors.joining(", ", "(", ")")));
    }
    text.append(" (\n");
    var columns = symbol.columns();
    for (var i = 0; i < Math.min(columns.size(), MAX_COLUMNS); i++) {
      var column = columns.get(i);
      text.append("  ").append(column.name()).append(' ').append(column.type());
      text.append(i + 1 < columns.size() ? ",\n" : "\n");
    }
    if (columns.size() > MAX_COLUMNS) {
      text.append("  -- ").append(columns.size() - MAX_COLUMNS).append(" more columns\n");
    }
    text.append(")\n```\n\n");
    text.append(description(symbol));
    symbol.documentation().ifPresent(doc -> text.append("\n\n").append(doc.strip()));
    return text.toString();
  }

  private static String column(ColumnTarget target) {
    var reference = target.reference();
    var name = String.join(".", reference.column());
    var column = target.table().flatMap(table -> table.column(reference.column().get(0)));
    var type =
        reference
            .type()
            .or(
                () ->
                    target
                        .table()
                        .flatMap(table -> table.column(reference.column()))
                        .map(Column::type));
    var text = new StringBuilder("```sql\n").append(name);
    type.ifPresent(t -> text.append(' ').append(t));
    text.append("\n```\n\n");
    var tableName = target.table().map(Symbol::name).orElse(reference.table().getObjectName());
    text.append(reference.column().size() > 1 ? "Field of column `" : "Column of `");
    if (reference.column().size() > 1) {
      text.append(reference.column().get(0)).append("` of `");
    }
    text.append(tableName).append('`');
    column.flatMap(Column::documentation).ifPresent(doc -> text.append("\n\n").append(doc.strip()));
    return text.toString();
  }

  private static String description(Symbol symbol) {
    var text = new StringBuilder();
    switch (symbol.kind()) {
      case TABLE -> text.append(capitalize(tableType(symbol.tableType())));
      case FUNCTION ->
          text.append("Table function returning a ").append(tableType(symbol.tableType()));
      case RELATIONSHIP ->
          text.append("Relationship of `")
              .append(symbol.name(), 0, Math.max(symbol.name().lastIndexOf('.'), 0))
              .append('`');
    }
    if (!symbol.primaryKey().isEmpty()) {
      text.append(", primary key ").append(codeList(symbol.primaryKey()));
    }
    var location = symbol.location();
    text.append(" · `")
        .append(location.file().getFileName())
        .append("` line ")
        .append(location.startLine() + 1);
    return text.toString();
  }

  private static String tableType(String tableType) {
    return switch (tableType) {
      case "STREAM" -> "stream";
      case "VERSIONED_STATE" -> "versioned state table";
      case "STATE" -> "state table";
      case "LOOKUP" -> "lookup table";
      case "STATIC" -> "static table";
      default -> "table";
    };
  }

  private static String codeList(List<String> names) {
    return names.stream().map(n -> "`" + n + "`").collect(Collectors.joining(", "));
  }

  private static String capitalize(String text) {
    return text.substring(0, 1).toUpperCase(Locale.ROOT) + text.substring(1);
  }
}
