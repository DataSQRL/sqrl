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

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_DATABASE;

import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.ScriptText.NamePart;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Column;
import com.datasqrl.compile.check.SymbolIndex.ImportLink;
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import lombok.experimental.UtilityClass;

/**
 * Finds what a position in a script refers to, using the symbol indexes of the last checks of the
 * projects that use the script.
 *
 * <p>The positions in an index are only valid for the content of the script that was checked. If
 * the script changed since, or the position is not in a query the planner resolved (e.g. the table
 * of an EXPORT), the name at the position is looked up instead.
 */
@UtilityClass
class SymbolLookup {

  sealed interface Target
      permits SymbolTarget, ColumnTarget, ImportTarget, ImportedTableTarget, DatabaseTarget {}

  /** A table, function or relationship. */
  record SymbolTarget(Symbol symbol) implements Target {}

  /**
   * A column of a table, or a nested field of a column, which is empty if the table is not in the
   * index.
   */
  record ColumnTarget(Reference reference, Optional<Symbol> table) implements Target {}

  /** What an IMPORT statement imports. */
  record ImportTarget(List<SourceRange> targets, List<Symbol> tables) implements Target {}

  /**
   * The definition of a table in a script that IMPORT statements import with an alias, e.g. {@code
   * CREATE TABLE Metrics} of {@code IMPORT events.Metrics AS _Metrics}. The aliases are the names
   * of other tables, which a rename of the definition would not change.
   *
   * @param definition the name of the table in its definition
   * @param imports the paths of the IMPORT statements that import it
   * @param tables the tables that the IMPORT statements define with their aliases
   */
  record ImportedTableTarget(SourceRange definition, List<SourceRange> imports, List<Symbol> tables)
      implements Target {}

  /**
   * A database, e.g. {@code ctl} of {@code ctl.Orders}, which is a script that is imported with its
   * name or an alias.
   *
   * @param name the name of the database
   * @param tables the tables, functions and relationships in it
   * @param scripts the start of the scripts that define it
   */
  record DatabaseTarget(String name, List<Symbol> tables, List<SourceRange> scripts)
      implements Target {}

  static Optional<Target> find(
      List<SymbolIndex> indexes, Path file, String text, int line, int character) {
    var script = new ScriptText(text);
    var offset = script.offset(line, character);
    for (var index : indexes) {
      if (!index.isCurrent(file, text)) {
        continue;
      }
      var reference = index.referenceAt(file, line, character);
      if (reference.isPresent()) {
        var table = index.symbol(reference.get().table());
        var parts = parts(script, reference.get().location());
        var part = partAt(parts, script, reference.get().location(), offset);
        if (reference.get().isColumn()) {
          return Optional.of(columnTarget(reference.get(), table, parts, part));
        }
        if (part >= 0 && part < parts.size() - 1) {
          // A qualifier of the table, e.g. its database
          var database = database(indexes, parts.get(part).name());
          if (database.isPresent()) {
            return database;
          }
        }
        if (table.isPresent()) {
          return Optional.of(new SymbolTarget(table.get()));
        }
      }
      var symbol = index.symbolAt(file, line, character);
      if (symbol.isPresent()) {
        return Optional.of(new SymbolTarget(symbol.get()));
      }
      var imports = index.importTargetsAt(file, line, character);
      if (!imports.isEmpty()) {
        // The name of an imported script, or its alias, is the name of a database
        return importedDatabase(indexes, script, offset)
            .or(() -> Optional.of(new ImportTarget(imports, importedTables(index, imports))));
      }
      var imported = importedTableAt(index, file, line, character);
      if (imported.isPresent()) {
        return imported;
      }
    }
    var name = script.nameAt(offset);
    if (name.isEmpty()) {
      return Optional.empty();
    }
    var parts = name.get();
    var last = parts.get(parts.size() - 1);
    if (parts.size() == 1 && last.end() < text.length() && text.charAt(last.end()) == '.') {
      var database = database(indexes, last.name());
      if (database.isPresent()) {
        return database;
      }
    }
    return findByName(indexes, file, parts.stream().map(NamePart::name).toList())
        .map(SymbolTarget::new);
  }

  /**
   * The column of a reference, or the nested field at the position, e.g. {@code address} of {@code
   * address.city}.
   */
  private static ColumnTarget columnTarget(
      Reference reference, Optional<Symbol> table, List<NamePart> parts, int part) {
    var column = reference.column();
    if (part < 0 || part >= column.size() - 1 || parts.size() != column.size()) {
      return new ColumnTarget(reference, table);
    }
    var path = column.subList(0, part + 1);
    var location = reference.location();
    var end = location.endCharacter() - (parts.get(parts.size() - 1).end() - parts.get(part).end());
    return new ColumnTarget(
        new Reference(
            new SourceRange(
                location.file(),
                location.startLine(),
                location.startCharacter(),
                location.endLine(),
                end),
            reference.table(),
            List.copyOf(path),
            table.flatMap(t -> t.column(path)).map(Column::type)),
        table);
  }

  /**
   * The tables that an IMPORT statement imports: the imported ones, or those of an imported script,
   * e.g. of {@code IMPORT shop.sources.*}, whose target is the start of the script.
   */
  private static List<Symbol> importedTables(SymbolIndex index, List<SourceRange> targets) {
    var scripts = new HashSet<Path>();
    targets.stream()
        .filter(
            target ->
                target.startLine() == 0
                    && target.startCharacter() == 0
                    && target.endLine() == 0
                    && target.endCharacter() == 0)
        .forEach(target -> scripts.add(target.file()));
    var tables = new LinkedHashMap<String, Symbol>();
    for (var symbol : index.symbols()) {
      if (targets.contains(symbol.location()) || scripts.contains(symbol.location().file())) {
        tables.putIfAbsent(symbol.name(), symbol);
      }
    }
    return List.copyOf(tables.values());
  }

  /**
   * The definition of a table at a position that IMPORT statements import with an alias, which
   * defines another table, so the definition is not the one of a table in the index.
   */
  private static Optional<Target> importedTableAt(
      SymbolIndex index, Path file, int line, int character) {
    var links =
        index.imports().stream()
            .filter(link -> !isEmpty(link.target()))
            .filter(link -> link.target().contains(file, line, character))
            .toList();
    if (links.isEmpty()) {
      return Optional.empty();
    }
    var tables =
        index.symbols().stream()
            .filter(table -> links.stream().anyMatch(link -> isAliasOf(table, link)))
            .toList();
    return Optional.of(
        new ImportedTableTarget(
            links.get(0).target(),
            links.stream().map(ImportLink::location).distinct().toList(),
            tables));
  }

  /** Whether a table is defined by the alias after the path of an IMPORT statement. */
  private static boolean isAliasOf(Symbol table, ImportLink link) {
    var alias = table.location();
    var path = link.location();
    return alias.file().equals(path.file())
        && alias.startLine() == path.endLine()
        && alias.startCharacter() >= path.endCharacter();
  }

  /** Whether a range is empty, e.g. the start of a script that is imported with {@code .*}. */
  private static boolean isEmpty(SourceRange range) {
    return range.startLine() == range.endLine() && range.startCharacter() == range.endCharacter();
  }

  /** The database that an IMPORT statement names at a position, if any. */
  private static Optional<Target> importedDatabase(
      List<SymbolIndex> indexes, ScriptText script, int offset) {
    var name = script.nameAt(offset);
    if (name.isEmpty()) {
      return Optional.empty();
    }
    var parts = name.get();
    var last = parts.get(parts.size() - 1);
    var end = last.end();
    var text = script.text();
    if (end < text.length() && text.charAt(end) == '.') {
      return Optional.empty(); // a package of the path
    }
    return database(indexes, last.name());
  }

  /**
   * The database with a name: its tables, and the scripts that the IMPORT statements of the indexes
   * import with that name.
   */
  static Optional<Target> database(List<SymbolIndex> indexes, String name) {
    var tables = new ArrayList<Symbol>();
    var names = new HashSet<String>();
    var scripts = new LinkedHashSet<SourceRange>();
    for (var index : indexes) {
      for (var symbol : index.symbols()) {
        if (symbol.identifier().getDatabaseName().equalsIgnoreCase(name)
            && names.add(symbol.name().toLowerCase(Locale.ROOT))) {
          tables.add(symbol);
        }
      }
      for (var link : index.imports()) {
        var target = link.target();
        var isScript =
            target.startLine() == 0
                && target.startCharacter() == 0
                && target.endLine() == 0
                && target.endCharacter() == 0;
        if (isScript && name.equalsIgnoreCase(lastName(index, link.location()))) {
          scripts.add(target);
        }
      }
    }
    if (tables.isEmpty()) {
      return Optional.empty();
    }
    if (scripts.isEmpty()) {
      tables.forEach(table -> scripts.add(SourceRange.at(table.location().file(), 0, 0)));
    }
    return Optional.of(new DatabaseTarget(name, List.copyOf(tables), List.copyOf(scripts)));
  }

  /** The last part of the name at a range of a checked file, e.g. the alias of an import. */
  private static String lastName(SymbolIndex index, SourceRange range) {
    var text = index.texts().get(range.file());
    if (text == null) {
      return "";
    }
    var parts = parts(new ScriptText(text), range);
    return parts.isEmpty() ? "" : parts.get(parts.size() - 1).name();
  }

  /** The parts of the name at a range, relative to the start of the range. */
  private static List<NamePart> parts(ScriptText script, SourceRange range) {
    var start = script.offset(range.startLine(), range.startCharacter());
    var end = script.offset(range.endLine(), range.endCharacter());
    if (end <= start) {
      return List.of();
    }
    return ScriptText.parseName(script.text().substring(start, end)).orElse(List.of());
  }

  /** The index of the part that contains an offset, or -1. */
  private static int partAt(
      List<NamePart> parts, ScriptText script, SourceRange range, int offset) {
    var start = script.offset(range.startLine(), range.startCharacter());
    for (var i = 0; i < parts.size(); i++) {
      if (offset >= start + parts.get(i).start() && offset <= start + parts.get(i).end()) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Finds a table, function or relationship by its name, e.g. {@code Orders}, {@code
   * sources.Orders} or {@code Customers.orders}.
   */
  static Optional<Symbol> findByName(List<SymbolIndex> indexes, Path file, List<String> names) {
    var name = names.get(names.size() - 1);
    for (var index : indexes) {
      var path = index.symbolsNamed(String.join(".", names));
      if (!path.isEmpty()) {
        return Optional.of(path.get(0));
      }
      var candidates = index.symbolsNamed(name);
      if (names.size() > 1) {
        var database = names.get(names.size() - 2);
        var qualified =
            candidates.stream()
                .filter(s -> s.identifier().getDatabaseName().equalsIgnoreCase(database))
                .findFirst();
        if (qualified.isPresent()) {
          return qualified;
        }
      }
      var database = index.databaseOf(file).orElse(FLINK_DEFAULT_DATABASE);
      var local =
          candidates.stream()
              .filter(s -> s.identifier().getDatabaseName().equals(database))
              .findFirst();
      if (local.isPresent()) {
        return local;
      }
      if (!candidates.isEmpty()) {
        return Optional.of(candidates.get(0));
      }
    }
    return Optional.empty();
  }
}
