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
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.lsp.SymbolLookup.ImportedTableTarget;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Finds where the scripts of the checked projects name a table, function, relationship or column:
 * its definition, and the references in queries, statements and IMPORT paths.
 *
 * <p>A script that is imported by several scripts or projects is planned once for each, so the same
 * definition can have several symbols, e.g. in different databases. They are all occurrences of the
 * same definition. The ranges only cover the name itself, e.g. {@code Orders} of {@code
 * sources.Orders}, and are positions in the content of the scripts when they were checked.
 */
class SymbolOccurrences {

  /**
   * A name in a script.
   *
   * @param range the name
   * @param definition whether it is the name in the definition
   */
  record Occurrence(SourceRange range, boolean definition) {}

  private final List<SymbolIndex> indexes;
  private final Map<Path, Optional<ScriptText>> texts = new HashMap<>();

  SymbolOccurrences(List<SymbolIndex> indexes) {
    this.indexes = indexes;
  }

  /** The last part of the name of a symbol, e.g. {@code orders} of {@code Customers.orders}. */
  static String simpleName(Symbol symbol) {
    var name = symbol.name();
    return name.substring(name.lastIndexOf('.') + 1);
  }

  /** Whether two symbols are the same definition, planned in different databases or projects. */
  static boolean sameDefinition(Symbol symbol, Symbol other) {
    return symbol.kind() == other.kind()
        && symbol.location().equals(other.location())
        && simpleName(symbol).equalsIgnoreCase(simpleName(other));
  }

  /** The symbols of an index that are the same definition as the given one. */
  static List<Symbol> sameDefinitions(SymbolIndex index, Symbol symbol) {
    return index.symbols().stream().filter(s -> sameDefinition(s, symbol)).toList();
  }

  /** The occurrences of a table, function or relationship. */
  List<Occurrence> of(Symbol symbol) {
    var name = simpleName(symbol);
    var occurrences = new LinkedHashSet<Occurrence>();
    for (var index : indexes) {
      var definitions = sameDefinitions(index, symbol);
      var identifiers = identifiers(definitions);
      for (var definition : definitions) {
        namePart(index, definition.location(), name)
            .ifPresent(range -> occurrences.add(new Occurrence(range, true)));
      }
      for (var reference : index.references()) {
        if (!reference.isColumn() && identifiers.contains(reference.table())) {
          namePart(index, reference.location(), name)
              .ifPresent(range -> occurrences.add(new Occurrence(range, false)));
        }
      }
      for (var link : index.imports()) {
        if (link.target().equals(symbol.location())) {
          namePart(index, link.location(), name)
              .ifPresent(range -> occurrences.add(new Occurrence(range, false)));
        }
      }
    }
    return List.copyOf(occurrences);
  }

  /**
   * The occurrences of a column: the statement that added it, if any, and the references to it.
   *
   * @param column a reference to the column
   * @param table the table of the column, if it is in an index
   */
  /**
   * The occurrences of a table that IMPORT statements import with aliases: its definition, the
   * IMPORT statements, and the occurrences of the tables that their aliases define.
   */
  List<Occurrence> of(ImportedTableTarget imported) {
    var occurrences = new LinkedHashSet<Occurrence>();
    occurrences.add(new Occurrence(imported.definition(), true));
    imported.imports().forEach(path -> occurrences.add(new Occurrence(path, false)));
    imported.tables().forEach(table -> occurrences.addAll(of(table)));
    return List.copyOf(occurrences);
  }

  List<Occurrence> of(Reference column, Optional<Symbol> table) {
    var occurrences = new LinkedHashSet<Occurrence>();
    for (var index : indexes) {
      var definitions = table.map(t -> sameDefinitions(index, t)).orElse(List.of());
      var identifiers = identifiers(definitions);
      identifiers.add(column.table());
      for (var definition : definitions) {
        definition.columnLocations().entrySet().stream()
            .filter(entry -> entry.getKey().equalsIgnoreCase(column.column().get(0)))
            .forEach(
                entry ->
                    namePart(index, entry.getValue(), entry.getKey())
                        .ifPresent(range -> occurrences.add(new Occurrence(range, true))));
      }
      for (var reference : index.references()) {
        if (identifiers.contains(reference.table()) && sameColumn(reference, column)) {
          occurrences.add(new Occurrence(reference.location(), false));
        }
      }
    }
    return List.copyOf(occurrences);
  }

  private static Set<ObjectIdentifier> identifiers(List<Symbol> symbols) {
    return symbols.stream().map(Symbol::identifier).collect(Collectors.toCollection(HashSet::new));
  }

  private static boolean sameColumn(Reference reference, Reference column) {
    var names = reference.column();
    if (names.size() != column.column().size()) {
      return false;
    }
    for (var i = 0; i < names.size(); i++) {
      if (!names.get(i).equalsIgnoreCase(column.column().get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * The range of the last part of the qualified name in a range, if it is the given name, e.g.
   * {@code Orders} of {@code sources.Orders}.
   */
  private Optional<SourceRange> namePart(SymbolIndex index, SourceRange range, String name) {
    var script =
        texts
            .computeIfAbsent(
                range.file(),
                file -> Optional.ofNullable(index.texts().get(file)).map(ScriptText::new))
            .orElse(null);
    if (script == null) {
      return Optional.empty();
    }
    var start = script.offset(range.startLine(), range.startCharacter());
    var end = script.offset(range.endLine(), range.endCharacter());
    if (end <= start) {
      return Optional.empty();
    }
    var parts = ScriptText.parseName(script.text().substring(start, end));
    if (parts.isEmpty()) {
      return Optional.empty();
    }
    var last = parts.get().get(parts.get().size() - 1);
    if (!last.name().equalsIgnoreCase(name)) {
      return Optional.empty();
    }
    var partStart = start + last.start();
    var partEnd = start + last.end();
    return Optional.of(
        new SourceRange(
            range.file(),
            script.lineOf(partStart),
            script.characterOf(partStart),
            script.lineOf(partEnd),
            script.characterOf(partEnd)));
  }
}
