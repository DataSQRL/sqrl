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

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.check.BuildPositions.BuildLocation;
import com.datasqrl.compile.check.BuildPositions.PendingImport;
import com.datasqrl.compile.check.BuildPositions.PendingName;
import com.datasqrl.compile.check.BuildPositions.PendingReference;
import com.datasqrl.compile.check.SymbolIndex.ApiFile;
import com.datasqrl.compile.check.SymbolIndex.Column;
import com.datasqrl.compile.check.SymbolIndex.ImportLink;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.packager.SourceFiles;
import com.datasqrl.plan.util.PrimaryKeyMap;
import com.datasqrl.planner.PlannerListener;
import com.datasqrl.planner.QueryReferences;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqlScriptStatementSplitter;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlDistinctStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlPassthroughTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlTableFunctionStatement;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Collects the symbols of a project while the planner plans it. Positions are recorded in the build
 * files the planner reads and mapped to the source files when the index is built, see {@link
 * BuildPositions}.
 */
@Slf4j
class SymbolCollector implements PlannerListener {

  private static final class Statement {
    final ParsedStatement parsed;
    final Path file;
    final FileLocation location;
    int validatedQueries = 0;

    Statement(ParsedStatement parsed, Path file, FileLocation location) {
      this.parsed = parsed;
      this.file = file;
      this.location = location;
    }

    SQLStatement sql() {
      return parsed.statement().get();
    }

    BuildLocation at(String name) {
      return new BuildLocation(file, location, name);
    }
  }

  private static final class PendingSymbol {
    final Kind kind;
    final ObjectIdentifier identifier;
    final String name;
    final BuildLocation location;
    // Where the statement that added the symbol is, if the definition is not in a source file
    final Optional<BuildLocation> fallback;
    String tableType;
    List<String> primaryKey;
    List<Column> columns;
    List<Column> parameters = List.of();
    Optional<String> documentation;
    boolean test;
    final Map<String, BuildLocation> columnLocations = new LinkedHashMap<>();

    PendingSymbol(
        Kind kind,
        ObjectIdentifier identifier,
        String name,
        BuildLocation location,
        Optional<BuildLocation> fallback) {
      this.kind = kind;
      this.identifier = identifier;
      this.name = name;
      this.location = location;
      this.fallback = fallback;
    }
  }

  private final Deque<Statement> statements = new ArrayDeque<>();
  private final Map<ObjectIdentifier, PendingSymbol> symbols = new LinkedHashMap<>();
  private final List<PendingReference> references = new ArrayList<>();
  private final List<PendingName> names = new ArrayList<>();
  private final Set<PendingImport> imports = new LinkedHashSet<>();
  private final List<QueryHints.Located> hintProblems = new ArrayList<>();
  private final Set<Path> plannedFiles = new LinkedHashSet<>();

  /** Whether a newer check replaces this one, which then stops at its next statement */
  private final BooleanSupplier outdated;

  SymbolCollector(BooleanSupplier outdated) {
    this.outdated = outdated;
  }

  @Override
  public void statementStarted(ParsedStatement statement, ErrorLocation location) {
    if (outdated.getAsBoolean()) {
      throw new CancellationException("A newer check replaces this one");
    }
    var file = location.getSourcePath().map(Path::of).orElse(null);
    var current =
        new Statement(
            statement, file, location.hasFile() ? location.getFileLocation() : FileLocation.START);
    if (file != null) {
      plannedFiles.add(file);
    }
    var parent = statements.peek();
    if (isImport(parent) && file != null && !file.equals(parent.file)) {
      // The first statement of an imported script
      addImport(parent, new BuildLocation(file, FileLocation.START, ""));
    }
    statements.push(current);
  }

  @Override
  public void statementFinished() {
    statements.poll();
  }

  @Override
  public void tableDefined(TableAnalysis table, ErrorLocation location) {
    var current = statements.peek();
    var identifier = table.getObjectIdentifier();
    var existing = symbols.get(identifier);
    if (existing != null
        && current != null
        && current.sql() instanceof SqrlAddColumnStatement addColumn) {
      // The table is replaced by one with the added column
      setTableDetails(existing, table);
      existing.columnLocations.put(
          addColumn.getPath().getLast().getDisplay(), current.at(addColumn.getPath().getDisplay()));
      return;
    }
    var name = identifier.getObjectName();
    var definition = toBuildLocation(location, name).orElse(null);
    if (definition == null && current == null) {
      return;
    }
    var fallback = Optional.ofNullable(current).map(s -> s.at(name));
    // A table that is imported with an alias is defined by the alias
    var aliased = isImport(current) && ((SqrlImportStatement) current.sql()).getAlias().isPresent();
    var symbol =
        new PendingSymbol(
            Kind.TABLE,
            identifier,
            name,
            definition != null && !aliased ? definition : fallback.get(),
            fallback);
    setTableDetails(symbol, table);
    symbols.put(identifier, symbol);
    if (isImport(current) && definition != null && !definition.file().equals(current.file)) {
      // A table imported from another script, which has the last name of the path there
      plannedFiles.add(definition.file());
      var imported =
          ((SqrlImportStatement) current.sql()).getPackageIdentifier().get().getLast().getDisplay();
      addImport(
          current,
          aliased
              ? new BuildLocation(definition.file(), definition.location(), imported)
              : definition);
    }
  }

  @Override
  public void functionDefined(SqrlTableFunction function, ErrorLocation location) {
    var current = statements.peek();
    var name = function.getFullPath().getDisplay();
    var definition =
        toBuildLocation(location, name).or(() -> Optional.ofNullable(current).map(s -> s.at(name)));
    if (definition.isEmpty()) {
      return;
    }
    var identifier = function.getFunctionAnalysis().getObjectIdentifier();
    var symbol =
        new PendingSymbol(
            function.isRelationship() ? Kind.RELATIONSHIP : Kind.FUNCTION,
            identifier,
            name,
            definition.get(),
            Optional.empty());
    var documentation = function.getDocumentation();
    symbol.tableType = function.getType().name();
    symbol.primaryKey = List.of();
    symbol.columns = IndexColumns.columns(function.getRowType(), documentation);
    symbol.parameters =
        function.getParameters().stream()
            .filter(
                p ->
                    p instanceof SqrlFunctionParameter sqrlParameter
                        && sqrlParameter.isExternalArgument())
            .map(SqrlFunctionParameter.class::cast)
            .map(
                p ->
                    new Column(
                        p.getName(),
                        IndexColumns.typeName(p.getRelDataType()),
                        documentation.getArgumentOpt(p.getName())))
            .toList();
    symbol.documentation = documentation.getDocStringOpt();
    symbol.test = function.getVisibility() != null && function.getVisibility().isTest();
    symbols.put(identifier, symbol);
    if (current != null && current.file != null) {
      try {
        if (current.sql() instanceof SqrlPassthroughTableFunctionStatement passthrough) {
          var tables =
              function.getFunctionAnalysis().getFromTables().stream()
                  .map(
                      table ->
                          new PassthroughReferences.Table(
                              table.getIdentifier().objectIdentifier(),
                              IndexColumns.fields(table.getRowType())))
                  .toList();
          addResolved(
              current,
              FunctionQueries.passthrough(
                  current.parsed.source(), passthrough, tables, parentTable(function)));
        } else if (current.sql() instanceof SqrlTableFunctionStatement relationship
            && function.isRelationship()) {
          parentTable(function)
              .ifPresent(
                  parent ->
                      addResolved(current, FunctionQueries.parentColumns(relationship, parent)));
        }
      } catch (RuntimeException e) {
        log.debug("Could not resolve the names of the query of a function", e);
      }
    }
  }

  /** Adds the names that were resolved in the query of a function. */
  private void addResolved(Statement statement, List<FunctionQueries.Resolved> resolved) {
    for (var name : resolved) {
      references.add(
          new PendingReference(
              statement.file,
              statement.location.add(name.start()),
              statement.location.add(name.end()),
              name.table(),
              name.column(),
              name.type()));
    }
  }

  /** The table that a relationship belongs to, with its columns. */
  private Optional<PassthroughReferences.Table> parentTable(SqrlTableFunction function) {
    if (!function.isRelationship()) {
      return Optional.empty();
    }
    var parentName = function.getFullPath().popLast().getDisplay();
    var database = function.getFunctionAnalysis().getObjectIdentifier().getDatabaseName();
    return symbols.values().stream()
        .filter(
            s ->
                s.kind == Kind.TABLE
                    && s.name.equalsIgnoreCase(parentName)
                    && s.identifier.getDatabaseName().equals(database)
                    && s.columns != null)
        .findFirst()
        .map(s -> new PassthroughReferences.Table(s.identifier, s.columns));
  }

  @Override
  public void tableReferenced(ObjectIdentifier table, ParsedObject<NamePath> name) {
    var current = statements.peek();
    if (current != null && current.file != null) {
      addName(current, name, table);
    }
  }

  @Override
  public void queryValidated(SqlNode query, SqlValidator validator) {
    var current = statements.peek();
    if (current == null || current.file == null) {
      return;
    }
    current.validatedQueries++;
    if (current.sql() instanceof SqrlDistinctStatement && current.validatedQueries > 1) {
      // The statement is planned again from SQL that was rewritten, without positions in the script
      return;
    }
    try {
      var statement = current.sql();
      var lastLine = Integer.MAX_VALUE;
      if (statement instanceof SqrlAddColumnStatement addColumn) {
        // The query of the table is planned again below the expression of the added column
        var expression =
            SqlScriptStatementSplitter.removeStatementDelimiter(
                addColumn.getDefinitionBody().get());
        lastLine = 1 + (int) expression.chars().filter(c -> c == '\n').count();
      }
      for (var reference : QueryReferences.collect(query, validator)) {
        var position = reference.position();
        var table = reference.table();
        if (position.getLineNum() > lastLine || table.size() != 3) {
          continue;
        }
        var identifier = ObjectIdentifier.of(table.get(0), table.get(1), table.get(2));
        if (statement instanceof SqrlDistinctStatement distinct && !reference.isColumn()) {
          // The table is in a FROM clause that the statement was rewritten into
          addName(current, distinct.getFrom(), identifier);
          continue;
        }
        var start =
            statement.mapSqlLocation(
                new FileLocation(position.getLineNum(), position.getColumnNum()));
        var end =
            statement.mapSqlLocation(
                new FileLocation(position.getEndLineNum(), position.getEndColumnNum()));
        references.add(
            new PendingReference(
                current.file,
                current.location.add(start),
                current.location.add(end),
                identifier,
                reference.column(),
                reference.type().map(IndexColumns::typeName)));
      }
      hintProblems.addAll(
          QueryHints.located(query, statement, current.file, current.location, lastLine));
    } catch (RuntimeException e) {
      log.debug("Could not resolve the names of a query", e);
    }
  }

  /** The options of hints in queries that name no input of their query, see {@link QueryHints}. */
  List<QueryHints.Located> hintProblems() {
    return hintProblems;
  }

  /** The build files of the scripts that were planned: the main script and those it imports. */
  Set<Path> plannedFiles() {
    return plannedFiles;
  }

  /**
   * Builds the index, with positions in the source files that the build files were created from.
   *
   * @param apiFiles the GraphQL files of the API of the project
   */
  SymbolIndex build(SourceFiles sourceFiles, List<ApiFile> apiFiles) {
    var builder = new BuildPositions(sourceFiles);
    var result = new ArrayList<Symbol>();
    for (var symbol : symbols.values()) {
      var location =
          builder
              .range(symbol.location)
              .or(() -> symbol.fallback.flatMap(builder::range))
              .orElse(null);
      if (location == null) {
        continue;
      }
      var columnLocations = new LinkedHashMap<String, SourceRange>();
      symbol.columnLocations.forEach(
          (column, columnLocation) ->
              builder.range(columnLocation).ifPresent(range -> columnLocations.put(column, range)));
      result.add(
          new Symbol(
              symbol.kind,
              symbol.identifier,
              symbol.name,
              location,
              symbol.tableType,
              symbol.primaryKey,
              symbol.columns,
              symbol.parameters,
              symbol.documentation,
              columnLocations,
              symbol.test));
    }
    var resolved = new LinkedHashSet<Reference>();
    for (var reference : references) {
      builder.reference(reference).ifPresent(resolved::add);
    }
    for (var name : names) {
      builder.name(name).ifPresent(resolved::add);
    }
    var links = new ArrayList<ImportLink>();
    for (var link : imports) {
      links.addAll(builder.importLinks(link));
    }
    var texts = new HashMap<Path, String>();
    for (var file : sourceFiles.getSourceFiles()) {
      try {
        texts.put(file, sourceFiles.read(file));
      } catch (IOException e) {
        log.debug("Could not read {}", file, e);
      }
    }
    return new SymbolIndex(
        List.copyOf(result),
        List.copyOf(resolved),
        List.copyOf(links),
        texts,
        List.copyOf(apiFiles));
  }

  private void addName(Statement statement, ParsedObject<NamePath> name, ObjectIdentifier table) {
    names.add(
        new PendingName(
            statement.file,
            statement.location.add(name.getFileLocation()),
            name.get().size(),
            table));
  }

  private void addImport(Statement importStatement, BuildLocation target) {
    imports.add(new PendingImport(importStatement.file, importStatement.location, target));
  }

  private static boolean isImport(Statement statement) {
    return statement != null
        && statement.file != null
        && statement.sql() instanceof SqrlImportStatement;
  }

  private static Optional<BuildLocation> toBuildLocation(ErrorLocation location, String name) {
    return location
        .getSourcePath()
        .map(
            path ->
                new BuildLocation(
                    Path.of(path),
                    location.hasFile() ? location.getFileLocation() : FileLocation.START,
                    name));
  }

  private static void setTableDetails(PendingSymbol symbol, TableAnalysis table) {
    var rowType = table.getRowType();
    var documentation = table.getDocumentation();
    symbol.tableType = table.getType().name();
    symbol.primaryKey = primaryKeyColumns(table.getPrimaryKey(), rowType);
    symbol.columns = IndexColumns.columns(rowType, documentation);
    symbol.documentation = documentation.getDocStringOpt();
    symbol.test = table.getHints().isTest();
  }

  private static List<String> primaryKeyColumns(PrimaryKeyMap primaryKey, RelDataType rowType) {
    if (!primaryKey.isDefined()) {
      return List.of();
    }
    try {
      return primaryKey.makeSimple(rowType).asSimpleList().stream()
          .map(index -> rowType.getFieldList().get(index).getName())
          .toList();
    } catch (RuntimeException e) {
      return List.of();
    }
  }
}
