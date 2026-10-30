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

import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlDefinition;
import com.datasqrl.planner.parser.SqrlExportStatement;
import com.datasqrl.planner.parser.SqrlImportStatement;
import com.datasqrl.planner.parser.SqrlPassthroughTableFunctionStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.planner.util.SqlTableNameExtractor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * Tracks the objects that failed to parse or plan, so that the knock-on errors of the statements
 * that use them (e.g. "Object 'X' not found") are not reported.
 *
 * <p>A statement that uses a failed object is still planned, and its error is only suppressed if
 * the validator could not find an object, column or function that failed, see {@link
 * #isKnockOnError}. Names are resolved by the validator, so a column or alias that is named like a
 * failed table is not confused with it, and the other errors of the statement are still reported.
 * The planner reports some errors before the validator runs, e.g. for a column that is added to a
 * failed table, so those statements are skipped before they are planned, see {@link #isDependent}.
 *
 * <p>Objects of imported scripts are tracked with their namespace (e.g. {@code sources.Orders}), so
 * that they are not confused with objects of the same name in other scripts. If the failed objects
 * are unknown, e.g. for an {@code IMPORT package.*} of a package that could not be loaded, no
 * object of that namespace is reported as not found.
 *
 * <p>A USE statement that failed or was skipped sets no catalog or database, so the statements
 * after it would run in another one than intended. They are all skipped then, see {@link
 * #isContextFailed()}.
 */
class FailedTables {

  /** Stands for all objects of a namespace, when the objects that failed are unknown */
  private static final String ALL = "*";

  private static final Pattern COMMENT_OR_STRING =
      Pattern.compile("/\\*.*?\\*/|--[^\\r\\n]*|'(?:[^']|'')*'", Pattern.DOTALL);
  private static final String IDENTIFIER_REGEX = "(?:`[^`]+`|[A-Za-z_][\\w$]*)";
  private static final Pattern IDENTIFIER = Pattern.compile(IDENTIFIER_REGEX);

  /** A name with optional qualifiers, e.g. {@code sources.Orders} */
  private static final String QUALIFIED_NAME_REGEX =
      IDENTIFIER_REGEX + "(?:\\s*\\.\\s*" + IDENTIFIER_REGEX + ")*";

  private static final Pattern CREATE_NAME =
      Pattern.compile(
          "^\\s*CREATE\\s+(?:TEMPORARY\\s+)?(?:SYSTEM\\s+)?(TABLE|VIEW|FUNCTION|CATALOG|DATABASE)\\s+"
              + "(?:IF\\s+NOT\\s+EXISTS\\s+)?("
              + QUALIFIED_NAME_REGEX
              + ")",
          Pattern.CASE_INSENSITIVE);

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

  /** A USE statement, which sets the catalog, database or modules of the statements after it */
  private static final Pattern USE = Pattern.compile("^\\s*USE\\s", Pattern.CASE_INSENSITIVE);

  private static final Pattern INSERT_TARGET =
      Pattern.compile(
          "^\\s*(?:EXECUTE\\s+)?INSERT\\s+(?:INTO|OVERWRITE)\\s+(" + QUALIFIED_NAME_REGEX + ")",
          Pattern.CASE_INSENSITIVE);

  // The messages of the validator for an object, column or function that is not found
  private static final Pattern OBJECT_NOT_FOUND =
      Pattern.compile("^(?:Object|Table) '([^']+)' not found(?: within '([^']+)')?");
  private static final Pattern COLUMN_NOT_FOUND = Pattern.compile("^Column '([^']+)' not found");
  private static final Pattern FUNCTION_NOT_FOUND =
      Pattern.compile("^No match found for function signature ([^(]+)\\(");

  /** The message of the catalog manager for a catalog or database that is not found */
  private static final Pattern CATALOG_NOT_FOUND =
      Pattern.compile("^A (?:catalog|database) with name \\[([^\\]]+)\\] does not exist");

  /** Tables, views, functions and imports that failed, with the namespace of their script */
  private final Set<String> names = new HashSet<>();

  /** Columns that failed to be added to a table */
  private final Set<String> columns = new HashSet<>();

  /** Catalogs and databases that failed, which do not belong to the namespace of a script */
  private final Set<String> catalogs = new HashSet<>();

  /** Whether a USE statement failed or was skipped */
  private boolean contextFailed = false;

  int size() {
    return names.size() + columns.size() + catalogs.size();
  }

  /**
   * Whether a USE statement failed or was skipped, so that the errors of the statements after it
   * would be knock-on errors of running in another catalog or database than intended.
   */
  boolean isContextFailed() {
    return contextFailed;
  }

  /**
   * Restores whether the context failed, e.g. after a script that is imported with a context of its
   * own.
   */
  void restoreContextFailed(boolean failed) {
    contextFailed = failed;
  }

  /**
   * Whether the error of a statement is a knock-on error of an object that failed, i.e. that the
   * validator could not find a table, column or function that failed, or that a USE statement could
   * not find a catalog or database that failed.
   *
   * @param error the error of planning the statement
   * @param namespace the namespace of the script that the statement belongs to, or null for the
   *     main script
   */
  boolean isKnockOnError(Throwable error, String namespace) {
    if (size() == 0) {
      return false;
    }
    var seen = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
    for (var cause = error; cause != null && seen.add(cause); cause = cause.getCause()) {
      if ((cause instanceof SqlValidatorException || cause instanceof CatalogException)
          && cause.getMessage() != null
          && isNotFoundOfFailed(cause.getMessage(), namespace)) {
        return true;
      }
    }
    return false;
  }

  private boolean isNotFoundOfFailed(String message, String namespace) {
    var object = OBJECT_NOT_FOUND.matcher(message);
    if (object.find()) {
      var name = object.group(1);
      if (object.group(2) != null) {
        // A qualified name that was not found within a catalog and database, e.g.
        // 'default_catalog.sources', whose database is the namespace of the script
        return isFailed(lastSegment(object.group(2)) + "." + name, null)
            || catalogs.contains(normalize(name));
      }
      return isFailed(name, namespace) || catalogs.contains(normalize(name));
    }
    var column = COLUMN_NOT_FOUND.matcher(message);
    if (column.find()) {
      return columns.contains(normalize(column.group(1)));
    }
    var function = FUNCTION_NOT_FOUND.matcher(message);
    if (function.find()) {
      return isFailed(function.group(1), namespace);
    }
    var catalog = CATALOG_NOT_FOUND.matcher(message);
    return catalog.find() && catalogs.contains(normalize(catalog.group(1)));
  }

  /**
   * Whether the statement adds to, exports or inserts into a table that failed. The planner reports
   * such an error before the statement is validated, so the statement is skipped instead.
   */
  boolean isDependent(SQLStatement statement, String statementSource, String namespace) {
    if (size() == 0) {
      return false;
    }
    if (statement instanceof SqrlDefinition definition
        && !definition.isTable()
        && isFailed(definition.getPath().getFirst().getDisplay(), namespace)) {
      // A column or relationship of a failed table
      return true;
    }
    if (statement instanceof SqrlPassthroughTableFunctionStatement function) {
      // Its query is not validated, but the planner looks up the tables of its FROM clauses
      return SqlTableNameExtractor.findTableNames(function.getDefinitionBody().get()).stream()
          .anyMatch(name -> isFailed(name, namespace));
    }
    if (statement instanceof SqrlExportStatement export) {
      return isFailedPath(export.getTableIdentifier().get().toStringList(), namespace)
          || isFailedPath(export.getPackageIdentifier().get().toStringList(), namespace);
    }
    var insert = INSERT_TARGET.matcher(withoutCommentsAndStrings(statementSource));
    return insert.find() && isFailedPath(segments(insert.group(1)), namespace);
  }

  /**
   * Whether a path as written in a statement, e.g. {@code sources.Orders}, names a failed object
   */
  private boolean isFailedPath(List<String> path, String namespace) {
    return isFailed(String.join(".", path), path.size() == 1 ? namespace : null)
        || catalogs.contains(normalize(path.get(0)));
  }

  /**
   * Whether the object failed, or all objects of its namespace (e.g. for a failed {@code IMPORT
   * package.*}), or the imported script that it belongs to.
   */
  private boolean isFailed(String name, String namespace) {
    var qualified = normalize(namespace == null ? name : namespace + "." + name);
    var lastDot = qualified.lastIndexOf('.');
    var parent = lastDot < 0 ? null : qualified.substring(0, lastDot);
    return names.contains(qualified)
        || names.contains(parent == null ? ALL : parent + "." + ALL)
        || (parent != null && names.contains(parent));
  }

  /**
   * Records the object defined by a statement that failed to plan.
   *
   * @param statement the statement that failed
   * @param statementSource the source text of the statement
   * @param recordedNestedFailures whether failures were recorded while planning the statement, e.g.
   *     in an imported script. Then only those objects failed, not the whole import.
   * @param namespace the namespace of the script's objects (e.g. {@code sources} for {@code IMPORT
   *     package.sources}), or null for the main script
   */
  void recordFailure(
      SQLStatement statement,
      String statementSource,
      boolean recordedNestedFailures,
      String namespace) {
    var withoutComments = withoutCommentsAndStrings(statementSource);
    if (USE.matcher(withoutComments).find()) {
      contextFailed = true;
    }
    if (statement instanceof SqrlAddColumnStatement addColumn) {
      columns.add(normalize(addColumn.getPath().getLast().getDisplay()));
    } else if (statement instanceof SqrlDefinition definition) {
      if (definition.isTable()) {
        add(namespace, definition.getPath().getDisplay());
      }
    } else if (statement instanceof SqrlImportStatement importStatement) {
      if (!recordedNestedFailures) {
        var alias = importStatement.getAlias();
        recordImport(
            namespace,
            importStatement.getPackageIdentifier().get().getLast().getDisplay(),
            alias.isPresent() ? alias.get().getLast().getDisplay() : null);
      }
    } else {
      recordCreated(withoutComments, namespace);
    }
  }

  /** Records the object defined by a statement that was skipped because it uses one that failed. */
  void recordSkipped(SQLStatement statement, String statementSource, String namespace) {
    recordFailure(statement, statementSource, false, namespace);
  }

  /**
   * Records the object defined by a statement that failed to parse, as far as its source tells,
   * e.g. {@code Customer} for {@code Customer := DISTINCT CustomerUpdates ON id;}.
   */
  void recordUnparsed(String statementSource, String namespace) {
    var withoutComments = withoutCommentsAndStrings(statementSource);
    var definition = DEFINITION.matcher(withoutComments);
    var importMatcher = IMPORT.matcher(withoutComments);
    if (definition.find()) {
      var path = segments(definition.group("tablename"));
      if (path.size() == 1) {
        add(namespace, path.get(0));
      } else if (path.size() == 2) {
        // A column, or a relationship that can't be told apart from one, but no query selects it
        columns.add(normalize(path.get(1)));
      }
    } else if (importMatcher.find()) {
      var path = segments(importMatcher.group(1));
      var alias = importMatcher.group(3);
      recordImport(
          namespace,
          importMatcher.group(2) != null ? ALL : path.get(path.size() - 1),
          alias == null ? null : segments(alias).get(0));
    } else {
      recordCreated(withoutComments, namespace);
    }
  }

  /**
   * Records an object of a package that failed to import, e.g. a table of an {@code IMPORT
   * package.*}, whose other objects are still imported.
   */
  void recordImported(String name, String namespace) {
    add(namespace, name);
  }

  private void recordImport(String namespace, String lastSegment, String alias) {
    // The objects of a package are unknown, and an alias would only be their prefix
    add(namespace, lastSegment.equals(ALL) ? ALL : alias != null ? alias : lastSegment);
  }

  private void recordCreated(String withoutComments, String namespace) {
    var matcher = CREATE_NAME.matcher(withoutComments);
    if (!matcher.find()) {
      return;
    }
    var path = segments(matcher.group(2));
    var name = path.get(path.size() - 1);
    var kind = matcher.group(1).toUpperCase(Locale.ROOT);
    if (kind.equals("CATALOG") || kind.equals("DATABASE")) {
      catalogs.add(normalize(name));
      return;
    }
    add(namespace, name);
    if (path.size() > 1) {
      // The validator resolves a qualified name within its database, e.g. mydb.Orders
      add(null, path.get(path.size() - 2) + "." + name);
    }
  }

  private void add(String namespace, String name) {
    if (!name.isBlank()) {
      names.add(normalize(namespace == null ? name : namespace + "." + name));
    }
  }

  private static String withoutCommentsAndStrings(String statementSource) {
    return COMMENT_OR_STRING.matcher(statementSource).replaceAll(" ");
  }

  private static List<String> segments(String qualifiedName) {
    var segments = new ArrayList<String>();
    var matcher = IDENTIFIER.matcher(qualifiedName);
    while (matcher.find()) {
      var segment = matcher.group();
      segments.add(segment.startsWith("`") ? segment.substring(1, segment.length() - 1) : segment);
    }
    return segments;
  }

  private static String lastSegment(String path) {
    return path.substring(path.lastIndexOf('.') + 1);
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
