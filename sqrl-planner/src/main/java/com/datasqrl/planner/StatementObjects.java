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
package com.datasqrl.planner;

import com.datasqrl.planner.parser.FlinkSQLStatement;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlExportStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlPassthroughTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.planner.util.SqlTableNameExtractor;
import com.google.common.base.Suppliers;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateDatabase;
import org.apache.flink.sql.parser.ddl.SqlCreateObject;
import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.sql.parser.ddl.SqlUseModules;
import org.apache.flink.sql.parser.ddl.catalog.SqlCreateCatalog;
import org.apache.flink.sql.parser.ddl.catalog.SqlUseCatalog;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

/**
 * What a statement defines and uses, as far as the knock-on errors of failed statements are
 * concerned, see {@link FailedObjects}.
 *
 * <p>It is read from the parsed statement, and for Flink SQL from the Flink parser. Only a
 * statement that failed to parse is read from its text, as far as that tells. The statement is read
 * when one of its parts is first needed, which is only once a statement failed.
 */
final class StatementObjects {

  /** An object that a statement defines, which fails when the statement fails */
  sealed interface Defined {}

  /** A table, view, function or imported object by its path, e.g. {@code mydb.Orders} */
  record Named(List<String> path) implements Defined {}

  /** The objects of an {@code IMPORT package.*}, which are unknown if the import fails */
  record AllImported() implements Defined {}

  /** A column that is added to a table */
  record Column(String name) implements Defined {}

  /** A catalog or database */
  record Catalog(String name) implements Defined {}

  private record Parts(List<Defined> defined, List<List<String>> targets, boolean isUse) {

    static final Parts NONE = new Parts(List.of(), List.of(), false);

    static Parts defining(Defined defined) {
      return new Parts(List.of(defined), List.of(), false);
    }
  }

  private static final Pattern COMMENT_OR_STRING =
      Pattern.compile("/\\*.*?\\*/|--[^\\r\\n]*|'(?:[^']|'')*'", Pattern.DOTALL);
  private static final String IDENTIFIER_REGEX = "(?:`[^`]+`|[A-Za-z_][\\w$]*)";
  private static final Pattern IDENTIFIER = Pattern.compile(IDENTIFIER_REGEX);

  /** A name with optional qualifiers, e.g. {@code sources.Orders} */
  private static final String QUALIFIED_NAME_REGEX =
      IDENTIFIER_REGEX + "(?:\\s*\\.\\s*" + IDENTIFIER_REGEX + ")*";

  // The statements that are recognized in the text of a statement that failed to parse

  /** A SQRL definition, e.g. {@code Orders := ...} or {@code Orders.total := ...} */
  private static final Pattern DEFINITION =
      Pattern.compile(
          SqrlStatementParser.SQRL_DEFINITION_REGEX, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /**
   * An IMPORT statement, e.g. {@code IMPORT package.Orders AS Order} or {@code IMPORT package.*}
   */
  private static final Pattern IMPORT =
      Pattern.compile(
          "^\\s*IMPORT\\s+("
              + QUALIFIED_NAME_REGEX
              + ")(\\s*\\.\\s*\\*)?(?:\\s+AS\\s+("
              + IDENTIFIER_REGEX
              + "))?",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern CREATE =
      Pattern.compile(
          "^\\s*CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:TEMPORARY\\s+)?(?:SYSTEM\\s+)?"
              + "(TABLE|VIEW|FUNCTION|CATALOG|DATABASE)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?("
              + QUALIFIED_NAME_REGEX
              + ")",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern USE = Pattern.compile("^\\s*USE\\s", Pattern.CASE_INSENSITIVE);

  private final Supplier<Parts> parts;

  private StatementObjects(Supplier<Parts> parts) {
    this.parts = Suppliers.memoize(parts::get);
  }

  /**
   * Reads a parsed statement.
   *
   * @param parseFlinkSql the parser of Flink SQL statements, also of CREATE TABLE statements
   */
  static StatementObjects of(SQLStatement statement, Function<String, SqlNode> parseFlinkSql) {
    return new StatementObjects(() -> read(statement, parseFlinkSql));
  }

  /** Reads a statement that failed to parse from its text */
  static StatementObjects ofUnparsed(String source) {
    return new StatementObjects(() -> readText(source));
  }

  /** The objects that the statement defines */
  List<Defined> defined() {
    return parts.get().defined();
  }

  /**
   * The tables, by their path as written, that the statement adds to, exports or inserts into, or
   * that the planner looks up before the statement is validated
   */
  List<List<String>> targets() {
    return parts.get().targets();
  }

  /** Whether it is a USE statement, which sets the catalog, database or modules after it */
  boolean isUse() {
    return parts.get().isUse();
  }

  /** Whether it adds a column to a table, or a relationship that can't be told apart from one */
  boolean addsColumn() {
    return defined().stream().anyMatch(Column.class::isInstance);
  }

  private static Parts read(SQLStatement statement, Function<String, SqlNode> parseFlinkSql) {
    if (statement instanceof SqrlDefinition definition) {
      return readDefinition(definition);
    }
    if (statement instanceof SqrlImportStatement importStatement) {
      var alias = importStatement.getAlias();
      return Parts.defining(
          imported(
              importStatement.getPackageIdentifier().get().getLast().getDisplay(),
              alias.isPresent() ? alias.get().getLast().getDisplay() : null));
    }
    if (statement instanceof SqrlExportStatement export) {
      return new Parts(
          List.of(),
          List.of(
              export.getTableIdentifier().get().toStringList(),
              export.getPackageIdentifier().get().toStringList()),
          false);
    }
    if (statement instanceof SqrlCreateTableStatement createTable) {
      return readFlinkSql(createTable.toSql(), parseFlinkSql);
    }
    if (statement instanceof FlinkSQLStatement flinkStatement) {
      return readFlinkSql(flinkStatement.sql().get(), parseFlinkSql);
    }
    return Parts.NONE;
  }

  private static Parts readDefinition(SqrlDefinition definition) {
    var path = definition.getPath();
    // The planner looks up the tables of a passthrough function before it plans its query
    var fromTables =
        definition instanceof SqrlPassthroughTableFunctionStatement function
            ? SqlTableNameExtractor.findTableNames(function.getDefinitionBody().get()).stream()
                .map(StatementObjects::segments)
                .toList()
            : List.<List<String>>of();
    if (definition.isTable()) {
      return new Parts(List.of(new Named(path.toStringList())), fromTables, false);
    }

    // A column or relationship, which the planner adds to its table before validating it
    var table = path.getFirst().getDisplay();
    var defined =
        definition instanceof SqrlAddColumnStatement
            ? List.<Defined>of(new Column(path.getLast().getDisplay()))
            : List.<Defined>of();
    var targets = Stream.concat(Stream.of(List.of(table)), fromTables.stream()).toList();
    return new Parts(defined, targets, false);
  }

  private static Parts readFlinkSql(String sql, Function<String, SqlNode> parseFlinkSql) {
    SqlNode node;
    try {
      node = parseFlinkSql.apply(sql);
    } catch (RuntimeException e) {
      // E.g. a syntax error, which is reported when the statement is planned
      return readText(sql);
    }

    if (node instanceof SqlUseCatalog
        || node instanceof SqlUseDatabase
        || node instanceof SqlUseModules) {
      return new Parts(List.of(), List.of(), true);
    }
    if (node instanceof SqlCreateCatalog createCatalog) {
      return Parts.defining(new Catalog(createCatalog.catalogName()));
    }
    if (node instanceof SqlCreateDatabase createDatabase) {
      return Parts.defining(new Catalog(last(List.of(createDatabase.getFullName()))));
    }
    if (node instanceof SqlCreateObject create) {
      // A table, view or function
      return Parts.defining(new Named(List.of(create.getFullName())));
    }
    if (node instanceof RichSqlInsert insert
        && insert.getTargetTableID() instanceof SqlIdentifier target) {
      return new Parts(List.of(), List.of(target.names), false);
    }
    return Parts.NONE;
  }

  private static Parts readText(String source) {
    var text = withoutCommentsAndStrings(source);

    var definition = DEFINITION.matcher(text);
    if (definition.find()) {
      var path = segments(definition.group("tablename"));
      return switch (path.size()) {
        case 1 -> Parts.defining(new Named(path));
        case 2 -> Parts.defining(new Column(path.get(1)));
        default -> Parts.NONE;
      };
    }

    var importStatement = IMPORT.matcher(text);
    if (importStatement.find()) {
      var alias = importStatement.group(3);
      return Parts.defining(
          imported(
              importStatement.group(2) != null ? "*" : last(segments(importStatement.group(1))),
              alias == null ? null : segments(alias).get(0)));
    }

    if (USE.matcher(text).find()) {
      return new Parts(List.of(), List.of(), true);
    }

    var create = CREATE.matcher(text);
    if (create.find()) {
      var path = segments(create.group(2));
      var kind = create.group(1).toUpperCase(Locale.ROOT);
      return Parts.defining(
          kind.equals("CATALOG") || kind.equals("DATABASE")
              ? new Catalog(last(path))
              : new Named(path));
    }
    return Parts.NONE;
  }

  /**
   * The object of an import of a package's object, whose alias is its name, or of all objects of a
   * package ({@code *}), whose alias would only be their prefix
   */
  private static Defined imported(String lastSegment, String alias) {
    if (lastSegment.equals("*")) {
      return new AllImported();
    }
    return new Named(List.of(alias != null ? alias : lastSegment));
  }

  private static String withoutCommentsAndStrings(String source) {
    return COMMENT_OR_STRING.matcher(source).replaceAll(" ");
  }

  /** The identifiers in the text, without quotes, e.g. {@code [sources, Orders]} */
  private static List<String> segments(String text) {
    var segments = new ArrayList<String>();
    var matcher = IDENTIFIER.matcher(text);
    while (matcher.find()) {
      var segment = matcher.group();
      segments.add(segment.startsWith("`") ? segment.substring(1, segment.length() - 1) : segment);
    }
    return segments;
  }

  private static String last(List<String> path) {
    return path.get(path.size() - 1);
  }
}
