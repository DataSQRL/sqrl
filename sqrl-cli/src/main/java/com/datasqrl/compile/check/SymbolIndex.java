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
package com.datasqrl.compile.check;

import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * The tables, functions and relationships of a checked project, where they are defined, and what
 * the names in its queries refer to. Positions are 0-based and ranges end exclusively, as in the
 * Language Server Protocol.
 *
 * @param symbols the defined tables, functions and relationships
 * @param references the names in queries that refer to tables or their columns
 * @param imports what the paths of IMPORT statements import
 * @param texts the content of the source files when they were checked, to tell whether the
 *     positions are still valid for the current content
 * @param apiFiles the GraphQL schemas and operations that define the API of the project
 */
public record SymbolIndex(
    List<Symbol> symbols,
    List<Reference> references,
    List<ImportLink> imports,
    Map<Path, String> texts,
    List<ApiFile> apiFiles) {

  public static final SymbolIndex EMPTY =
      new SymbolIndex(List.of(), List.of(), List.of(), Map.of(), List.of());

  public enum Kind {
    TABLE,
    FUNCTION,
    RELATIONSHIP
  }

  /** A range in a source file. */
  public record SourceRange(
      Path file, int startLine, int startCharacter, int endLine, int endCharacter) {

    public static SourceRange at(Path file, int line, int character) {
      return new SourceRange(file, line, character, line, character);
    }

    public boolean contains(Path otherFile, int line, int character) {
      return file.equals(otherFile)
          && compare(line, character, startLine, startCharacter) >= 0
          && compare(line, character, endLine, endCharacter) <= 0;
    }

    private static int compare(int line, int character, int otherLine, int otherCharacter) {
      return line != otherLine
          ? Integer.compare(line, otherLine)
          : Integer.compare(character, otherCharacter);
    }
  }

  /**
   * A table, table function or relationship.
   *
   * @param kind what it is
   * @param identifier its identifier in the catalog; the name of a relationship is its path
   * @param name its name in the script, e.g. {@code Customers} or {@code Customers.orders}
   * @param location its name in the statement that defines it
   * @param tableType the type of the table, e.g. {@code STREAM} or {@code STATE}
   * @param primaryKey the names of the primary key columns
   * @param columns the columns, for functions and relationships those of the result
   * @param parameters the parameters of a function
   * @param documentation the documentation comment of the definition
   * @param columnLocations the columns that were added by a separate statement, and its location
   * @param test whether it is only planned for tests, i.e. has a {@code test} hint
   */
  public record Symbol(
      Kind kind,
      ObjectIdentifier identifier,
      String name,
      SourceRange location,
      String tableType,
      List<String> primaryKey,
      List<Column> columns,
      List<Column> parameters,
      Optional<String> documentation,
      Map<String, SourceRange> columnLocations,
      boolean test) {

    public Optional<Column> column(String columnName) {
      return Column.named(columns, columnName);
    }

    /** A column or a nested field of a column, e.g. {@code [address, city]}. */
    public Optional<Column> column(List<String> path) {
      var column = column(path.get(0));
      for (var i = 1; i < path.size() && column.isPresent(); i++) {
        column = column.get().field(path.get(i));
      }
      return column;
    }
  }

  /**
   * A column or parameter.
   *
   * @param name the name
   * @param type the type, e.g. {@code BIGINT NOT NULL}
   * @param documentation the documentation, if any
   * @param fields the fields of a column of a row type
   */
  public record Column(
      String name, String type, Optional<String> documentation, List<Column> fields) {

    public Column(String name, String type, Optional<String> documentation) {
      this(name, type, documentation, List.of());
    }

    public Optional<Column> field(String fieldName) {
      return named(fields, fieldName);
    }

    static Optional<Column> named(List<Column> columns, String name) {
      return columns.stream().filter(c -> c.name().equalsIgnoreCase(name)).findFirst();
    }
  }

  /**
   * A name in a query that refers to a table or one of its columns.
   *
   * @param location the name; for a qualified column only the column part, e.g. {@code amount} of
   *     {@code o.amount}
   * @param table the table
   * @param column the column and nested field names, empty for a reference to the table
   * @param type the type of the column
   */
  public record Reference(
      SourceRange location, ObjectIdentifier table, List<String> column, Optional<String> type) {

    public boolean isColumn() {
      return !column.isEmpty();
    }
  }

  /**
   * The path of an IMPORT statement and what it imports.
   *
   * @param location the imported path in the IMPORT statement
   * @param target the start of an imported script, or the definition of an imported table
   */
  public record ImportLink(SourceRange location, SourceRange target) {}

  /**
   * A GraphQL file of the API.
   *
   * @param file the source file
   * @param schema whether it is a schema, otherwise it contains operations
   */
  public record ApiFile(Path file, boolean schema) {}

  public Optional<Symbol> symbol(ObjectIdentifier identifier) {
    return symbols.stream().filter(s -> s.identifier().equals(identifier)).findFirst();
  }

  /** The symbols with a name, ignoring case. */
  public List<Symbol> symbolsNamed(String name) {
    return symbols.stream().filter(s -> s.name().equalsIgnoreCase(name)).toList();
  }

  /** The innermost name in a query at a position. */
  public Optional<Reference> referenceAt(Path file, int line, int character) {
    return references.stream()
        .filter(r -> r.location().contains(file, line, character))
        .min(Comparator.comparingInt(r -> length(r.location())));
  }

  /** The symbol whose name in its definition is at a position. */
  public Optional<Symbol> symbolAt(Path file, int line, int character) {
    return symbols.stream().filter(s -> s.location().contains(file, line, character)).findFirst();
  }

  /** What the IMPORT statement at a position imports. */
  public List<SourceRange> importTargetsAt(Path file, int line, int character) {
    return imports.stream()
        .filter(i -> i.location().contains(file, line, character))
        .map(ImportLink::target)
        .distinct()
        .toList();
  }

  /** The database of the tables that are defined in a file, if any. */
  public Optional<String> databaseOf(Path file) {
    return symbols.stream()
        .filter(s -> s.location().file().equals(file))
        .map(s -> s.identifier().getDatabaseName())
        .findFirst();
  }

  /** Whether the positions in a file are valid for its current content. */
  public boolean isCurrent(Path file, String text) {
    return Objects.equals(texts.get(file), text);
  }

  private static int length(SourceRange range) {
    return range.startLine() == range.endLine()
        ? range.endCharacter() - range.startCharacter()
        : Integer.MAX_VALUE;
  }
}
