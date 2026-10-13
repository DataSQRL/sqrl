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

import com.datasqrl.compile.check.PlannedCatalog;
import com.datasqrl.compile.check.PlannedCatalog.Completions;
import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.ScriptText.NamePart;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import com.datasqrl.compile.check.SymbolIndex.Symbol;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.planner.hint.PlannerHint;
import com.datasqrl.planner.parser.SqrlDistinctStatement;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionItemKind;

/**
 * Suggests how to complete a script at a position. Queries are completed by the compiler with the
 * catalog of the last check, which is combined with the symbols of the last check and the files of
 * its build directory for the parts of a script that are specific to SQRL.
 */
@Slf4j
@RequiredArgsConstructor
class ScriptCompletion {

  private static final List<String> STATEMENT_KEYWORDS =
      List.of("IMPORT", "EXPORT", "CREATE TABLE", "NEXT_BATCH");
  private static final SqlAbstractParserImpl.Metadata SQL_METADATA =
      FlinkSqlParserImpl.FACTORY.getParser(new StringReader("")).getMetadata();
  private static final Pattern SIMPLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  private static final Pattern KEYWORD = Pattern.compile("[A-Za-z_][A-Za-z_ ]*");

  /** A table in the FROM clause of a query, with an optional alias */
  private static final Pattern FROM_TABLE =
      Pattern.compile(
          "\\b(?:FROM|JOIN)\\s+(?:LATERAL\\s+)?([\\w.`\"]+)"
              + "(?:\\s+FOR\\s+SYSTEM_TIME\\s+AS\\s+OF\\s+[\\w.`]+)?(?:\\s+(?:AS\\s+)?([\\w`\"]+))?",
          Pattern.CASE_INSENSITIVE);

  /**
   * A partially typed name after qualifiers, e.g. {@code o.am} or the nested field {@code
   * o.address.ci}
   */
  private static final Pattern QUALIFIED_PREFIX =
      Pattern.compile("((?:(?:[\\w$]+|`[^`]+`|\"[^\"]+\")\\.)+)(\\w*)$");

  private static final Pattern CREATE_TABLE_NAME =
      Pattern.compile(
          "\\bCREATE\\s+(?:TEMPORARY\\s+)?TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?([\\w`]+)",
          Pattern.CASE_INSENSITIVE);

  private static final int MIN_FUNCTIONS_OF_EXPRESSION = 10;

  private static volatile List<String> hintNames;

  // Sort columns first, then tables, databases, functions and keywords
  private enum Order {
    STATEMENT,
    COLUMN,
    ALIAS,
    TABLE,
    MODULE,
    FUNCTION,
    KEYWORD
  }

  private final Path file;
  private final Optional<PlannedCatalog> catalog;
  private final List<SymbolIndex> indexes;
  private final Optional<Path> buildDir;
  private final Map<String, CompletionItem> items = new LinkedHashMap<>();

  List<CompletionItem> complete(CompletionContext context) {
    switch (context.kind()) {
      case STATEMENT -> {
        STATEMENT_KEYWORDS.forEach(
            keyword -> add(keyword, CompletionItemKind.Keyword, null, Order.STATEMENT));
        tables(List.of(), false).forEach(this::addTable);
      }
      case KEYWORD -> context.names().forEach(this::addKeyword);
      case HINT ->
          hintNames().forEach(name -> add(name, CompletionItemKind.Keyword, "hint", Order.KEYWORD));
      case NAME ->
          context
              .names()
              .forEach(
                  name -> add(name, CompletionItemKind.Variable, "table or alias", Order.ALIAS));
      case IMPORT -> addModules(context.path(), true);
      case EXPORT_SINK -> addSinks(context.path());
      case TABLE -> addTables(context.path(), context.testStatement());
      case COLUMN -> {
        table(List.of(context.table().split("\\."))).ifPresent(this::addColumns);
        context.names().forEach(this::addKeyword);
      }
      case HINT_COLUMN -> {
        table(List.of(context.table().split("\\."))).ifPresent(this::addColumns);
        // Hints name columns as written, e.g. a column `timestamp` as timestamp
        items.values().forEach(item -> item.setInsertText(null));
      }
      case PARAMETER ->
          context
              .names()
              .forEach(name -> add(name, CompletionItemKind.Variable, "parameter", Order.COLUMN));
      case QUERY -> addQuery(context);
      // Templates are completed with the variables of the project, see TemplateVariables
      case NONE, TEMPLATE -> {}
    }
    return List.copyOf(items.values());
  }

  private void addQuery(CompletionContext context) {
    var database = database();
    var completions =
        catalog
            .map(c -> c.complete(context.sql(), context.sqlCursor(), database))
            .orElse(Completions.NONE);
    var functions = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
    catalog.ifPresent(c -> functions.addAll(c.functionNames()));
    var fromTables = fromTables(context.sql());
    // After a qualifier, e.g. `o.`, only columns and tables follow
    var qualified =
        QUALIFIED_PREFIX.matcher(context.sql().substring(0, context.sqlCursor())).find();
    var hasTables = false;
    var hasColumns = false;
    // Keywords that are also functions, e.g. LEFT
    var functionKeywords = new ArrayList<String>();
    for (var suggestion : completions.suggestions()) {
      var names = suggestion.names();
      switch (suggestion.type()) {
        case TABLE, VIEW -> {
          if (names.size() == 1) {
            if (table(names).isEmpty()) {
              add(names.get(0), CompletionItemKind.Variable, "alias", Order.ALIAS);
            }
          } else if (names.size() == 3) {
            var table = symbol(names.get(1), names.get(2));
            // Leave out internal tables, and test tables outside of tests
            if (table.isPresent() && (!table.get().test() || context.testStatement())) {
              addTable(table.get());
              hasTables = true;
            }
          }
        }
        case SCHEMA -> {
          if (names.size() == 2 && !names.get(1).equals(database)) {
            add(names.get(1), CompletionItemKind.Module, "database", Order.MODULE);
          }
        }
        case COLUMN -> {
          var name = suggestion.name();
          var type =
              fromTables.values().stream()
                  .flatMap(table -> table.column(name).stream())
                  .findFirst()
                  .map(SymbolIndex.Column::type)
                  .orElse(null);
          add(name, CompletionItemKind.Field, type, Order.COLUMN);
          hasColumns = true;
        }
        case FUNCTION -> {
          if (!qualified) {
            add(suggestion.name(), CompletionItemKind.Function, null, Order.FUNCTION);
          }
        }
        case KEYWORD -> {
          var name = suggestion.name();
          if (qualified) {
            continue;
          } else if (functions.contains(name)) {
            functionKeywords.add(name);
          } else if (KEYWORD.matcher(name).matches()) {
            addKeyword(name);
          }
        }
        default -> {}
      }
    }
    // Some keywords of clauses are also functions, e.g. LEFT, but in expressions there are many
    var isExpression = functionKeywords.size() >= MIN_FUNCTIONS_OF_EXPRESSION;
    functionKeywords.forEach(
        name ->
            add(
                name,
                isExpression ? CompletionItemKind.Function : CompletionItemKind.Keyword,
                null,
                isExpression ? Order.FUNCTION : Order.KEYWORD));
    if (!hasTables
        && !hasColumns
        && (completions.suggestions().isEmpty() || isExpression || qualified)) {
      // The compiler cannot complete the names in all expressions, e.g. in GROUP BY or the period
      // of a temporal join, where it suggests nothing, only functions or only `*` after a name
      hasColumns = addNamesFromClause(context, fromTables, catalog.isEmpty());
    }
    if (hasColumns && !qualified) {
      // The compiler suggests functions only in some expressions
      functions.forEach(name -> add(name, CompletionItemKind.Function, null, Order.FUNCTION));
    }
  }

  /**
   * Suggests the columns of the tables in the FROM clause of the query, or of the table that the
   * name before the position is qualified with.
   *
   * @return whether the position is in an expression
   */
  private boolean addNamesFromClause(
      CompletionContext context, Map<String, Symbol> fromTables, boolean noCatalog) {
    var beforeCursor = context.sql().substring(0, context.sqlCursor());
    var qualified = QUALIFIED_PREFIX.matcher(beforeCursor);
    if (qualified.find()) {
      var path =
          ScriptText.parseName(qualified.group(1).substring(0, qualified.group(1).length() - 1))
              .map(parts -> parts.stream().map(NamePart::name).toList())
              .orElse(List.of());
      if (path.isEmpty()) {
        return false;
      }
      var table = fromTables.get(path.get(0).toLowerCase(Locale.ROOT));
      if (table != null) {
        if (path.size() == 1) {
          addColumns(table);
        } else {
          table.column(path.subList(1, path.size())).ifPresent(this::addFields);
        }
        return false;
      }
      // The nested fields of a column, e.g. `address.`
      var column = fromTables.values().stream().flatMap(t -> t.column(path).stream()).findFirst();
      if (column.isPresent()) {
        addFields(column.get());
      } else if (path.size() == 1) {
        // A database, e.g. `sources.`
        tables(path, context.testStatement()).forEach(this::addTable);
      }
      return false;
    }
    fromTables.values().stream().distinct().forEach(this::addColumns);
    if (fromTables.isEmpty()) {
      tables(List.of(), context.testStatement()).forEach(this::addTable);
    }
    return noCatalog || !fromTables.isEmpty();
  }

  /** The tables in FROM clauses of a query by their alias and name, in lower case. */
  private Map<String, Symbol> fromTables(String sql) {
    var result = new LinkedHashMap<String, Symbol>();
    var matcher = FROM_TABLE.matcher(sql);
    while (matcher.find()) {
      // Names are quoted with backticks, or with double quotes in the SQL of a database
      var names = List.of(matcher.group(1).replaceAll("[`\"]", "").split("\\."));
      var table = table(names);
      if (table.isEmpty()) {
        continue;
      }
      result.putIfAbsent(names.get(names.size() - 1).toLowerCase(Locale.ROOT), table.get());
      var alias = matcher.group(2);
      if (alias != null && !SQL_METADATA.isReservedWord(alias.toUpperCase(Locale.ROOT))) {
        result.put(alias.replaceAll("[`\"]", "").toLowerCase(Locale.ROOT), table.get());
      }
    }
    return result;
  }

  /** Adds the scripts, tables and functions that can be imported at a path. */
  private void addModules(List<String> path, boolean imports) {
    if (buildDir.isEmpty()) {
      return;
    }
    var dir = buildDir.get();
    for (var name : path) {
      dir = dir.resolve(name);
    }
    if (Files.isDirectory(dir)) {
      try (var entries = Files.list(dir)) {
        for (var entry : entries.sorted().toList()) {
          addModule(entry, path.isEmpty(), imports);
        }
      } catch (IOException e) {
        log.debug("Could not list {}", dir, e);
      }
    } else if (imports && !path.isEmpty()) {
      // The tables of a script
      var script = dir.resolveSibling(dir.getFileName() + ModuleLoaderImpl.SQRL_FILE_EXTENSION);
      if (Files.isRegularFile(script)) {
        try {
          var matcher = CREATE_TABLE_NAME.matcher(Files.readString(script));
          while (matcher.find()) {
            add(matcher.group(1).replace("`", ""), CompletionItemKind.Class, "table", Order.TABLE);
          }
          addKeyword("*");
        } catch (IOException e) {
          log.debug("Could not read {}", script, e);
        }
      }
    }
  }

  private void addModule(Path entry, boolean root, boolean imports) {
    var name = entry.getFileName().toString();
    if (name.startsWith(".")) {
      return;
    }
    if (Files.isDirectory(entry)) {
      if (!(root && name.equals(SqrlConstants.DEPLOY_DIR_NAME)) && containsModules(entry)) {
        add(name, CompletionItemKind.Module, "package", Order.MODULE);
      }
    } else if (name.endsWith(ModuleLoaderImpl.TABLE_FILE_SUFFIX)) {
      add(
          strip(name, ModuleLoaderImpl.TABLE_FILE_SUFFIX),
          CompletionItemKind.Class,
          "table",
          Order.TABLE);
    } else if (imports && name.endsWith(ModuleLoaderImpl.SQRL_FILE_EXTENSION)) {
      // The script that is edited cannot import itself
      if (!(root && name.equals(file.getFileName().toString()))) {
        add(
            strip(name, ModuleLoaderImpl.SQRL_FILE_EXTENSION),
            CompletionItemKind.Module,
            "script",
            Order.MODULE);
      }
    } else if (imports && name.endsWith(ModuleLoaderImpl.FUNCTION_JSON_EXTENSION)) {
      add(
          strip(name, ModuleLoaderImpl.FUNCTION_JSON_EXTENSION),
          CompletionItemKind.Function,
          "function",
          Order.FUNCTION);
    }
  }

  /** Whether a directory contains scripts, tables or functions that can be imported. */
  private static boolean containsModules(Path dir) {
    try (var files = Files.walk(dir, 4)) {
      return files
          .map(f -> f.getFileName().toString())
          .anyMatch(
              name ->
                  name.endsWith(ModuleLoaderImpl.SQRL_FILE_EXTENSION)
                      || name.endsWith(ModuleLoaderImpl.TABLE_FILE_SUFFIX)
                      || name.endsWith(ModuleLoaderImpl.FUNCTION_JSON_EXTENSION));
    } catch (IOException e) {
      return false;
    }
  }

  /** Adds the built-in sinks and the tables that can be exported to. */
  private void addSinks(List<String> path) {
    var builtIn =
        Stream.of(SystemBuiltInConnectors.values()).map(SystemBuiltInConnectors::toString).toList();
    if (path.isEmpty()) {
      builtIn.forEach(name -> add(name, CompletionItemKind.Module, "built-in sink", Order.MODULE));
    } else if (path.size() == 1 && builtIn.contains(path.get(0).toLowerCase(Locale.ROOT))) {
      return; // the name of the sink is chosen freely
    }
    addModules(path, false);
  }

  private void addTables(List<String> path, boolean test) {
    if (path.isEmpty()) {
      // Tables of other databases are qualified with the database
      allSymbols()
          .filter(s -> s.kind() == Kind.TABLE)
          .map(s -> s.identifier().getDatabaseName())
          .filter(database -> !database.equals(database()))
          .distinct()
          .forEach(database -> add(database, CompletionItemKind.Module, "database", Order.MODULE));
    }
    tables(path, test).forEach(this::addTable);
  }

  /** The tables of a database, by default the one of the script. */
  private List<Symbol> tables(List<String> database, boolean test) {
    var name = database.isEmpty() ? database() : database.get(database.size() - 1);
    return allSymbols()
        .filter(s -> s.kind() == Kind.TABLE)
        .filter(s -> s.identifier().getDatabaseName().equalsIgnoreCase(name))
        .filter(s -> test || !s.test())
        .toList();
  }

  private Optional<Symbol> table(List<String> names) {
    return SymbolLookup.findByName(indexes, file, names).filter(s -> s.kind() == Kind.TABLE);
  }

  private Optional<Symbol> symbol(String database, String name) {
    return allSymbols()
        .filter(s -> s.identifier().getDatabaseName().equals(database))
        .filter(s -> s.identifier().getObjectName().equals(name))
        .findFirst();
  }

  private Stream<Symbol> allSymbols() {
    return indexes.stream().map(SymbolIndex::symbols).flatMap(Collection::stream);
  }

  /** The database of the script, whose tables are referenced without a database name. */
  private String database() {
    return indexes.stream()
        .flatMap(index -> index.databaseOf(file).stream())
        .findFirst()
        .orElse(FLINK_DEFAULT_DATABASE);
  }

  static List<String> hintNames() {
    if (hintNames == null) {
      var names = new TreeSet<String>();
      ServiceLoaderDiscovery.getAll(PlannerHint.Factory.class)
          .forEach(factory -> names.add(factory.getName()));
      names.add(SqrlDistinctStatement.FILTERED_DISTINCT_HINT_NAME);
      hintNames = List.copyOf(names);
    }
    return hintNames;
  }

  private void addTable(Symbol table) {
    add(
        table.identifier().getObjectName(),
        CompletionItemKind.Class,
        table.tableType().toLowerCase(Locale.ROOT).replace('_', ' '),
        Order.TABLE);
  }

  private void addColumns(Symbol table) {
    table.columns().forEach(c -> add(c.name(), CompletionItemKind.Field, c.type(), Order.COLUMN));
  }

  /** Adds the nested fields of a column of a row type. */
  private void addFields(SymbolIndex.Column column) {
    column.fields().forEach(f -> add(f.name(), CompletionItemKind.Field, f.type(), Order.COLUMN));
  }

  private void addKeyword(String keyword) {
    add(keyword, CompletionItemKind.Keyword, null, Order.KEYWORD);
  }

  private void add(String name, CompletionItemKind kind, String detail, Order order) {
    // Functions and keywords are case-insensitive, unlike tables and columns
    var isName = kind != CompletionItemKind.Keyword && kind != CompletionItemKind.Function;
    var key = (isName ? name : name.toLowerCase(Locale.ROOT)) + "/" + kind;
    if (items.containsKey(key)) {
      return;
    }
    var item = new CompletionItem(name);
    item.setKind(kind);
    item.setDetail(detail);
    item.setSortText(order.ordinal() + name.toLowerCase(Locale.ROOT));
    if (isName && needsQuotes(name)) {
      item.setInsertText("`" + name + "`");
    }
    items.put(key, item);
  }

  static boolean needsQuotes(String name) {
    return !SIMPLE_NAME.matcher(name).matches()
        || SQL_METADATA.isReservedWord(name.toUpperCase(Locale.ROOT));
  }

  private static String strip(String name, String suffix) {
    return name.substring(0, name.length() - suffix.length());
  }
}
