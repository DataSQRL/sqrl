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

import com.datasqrl.planner.StatementObjects.AllImported;
import com.datasqrl.planner.StatementObjects.Catalog;
import com.datasqrl.planner.StatementObjects.Column;
import com.datasqrl.planner.StatementObjects.Named;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.table.catalog.exceptions.CatalogException;

/**
 * The objects whose statements failed to parse or plan, so that the knock-on errors of the
 * statements that use them (e.g. "Object 'X' not found") are not reported.
 *
 * <p>A statement that uses a failed object is still planned, and its error is only suppressed if
 * the validator could not find an object, column or function that failed, see {@link
 * #isKnockOnError}. Names are resolved by the validator, so a column or alias that is named like a
 * failed table is not confused with it, and an error that does not come from a failed object is
 * still reported. Planning a statement stops at its first error though, so if that is a knock-on
 * error, the other errors of the statement only show once the failed object is fixed. The planner
 * reports some errors before the validator runs, e.g. for a column that is added to a failed table,
 * so those statements are skipped before they are planned, see {@link #isDependent}.
 *
 * <p>An instance is the view of the failures from one script. The objects of an imported script are
 * recorded with its namespace (e.g. {@code sources.Orders}), so that they are not confused with
 * objects of the same name in other scripts, see {@link #forImportedScript}. If the failed objects
 * are unknown, e.g. for an {@code IMPORT package.*} of a package that could not be loaded, no
 * object of that namespace is reported as not found.
 *
 * <p>A USE statement that failed or was skipped sets no catalog or database, so the statements
 * after it in the script would run in another one than intended. They are all skipped then, see
 * {@link #isContextFailed()}.
 */
final class FailedObjects {

  /** Stands for all objects of a namespace, when the objects that failed are unknown */
  private static final String ALL = "*";

  // The messages of the validator for an object, column or function that is not found
  private static final Pattern OBJECT_NOT_FOUND =
      Pattern.compile("^(?:Object|Table) '([^']+)' not found(?: within '([^']+)')?");
  private static final Pattern COLUMN_NOT_FOUND = Pattern.compile("^Column '([^']+)' not found");
  private static final Pattern FUNCTION_NOT_FOUND =
      Pattern.compile("^No match found for function signature ([^(]+)\\(");

  /** The message of the catalog manager for a catalog or database that is not found */
  private static final Pattern CATALOG_NOT_FOUND =
      Pattern.compile("^A (?:catalog|database) with name \\[([^\\]]+)\\] does not exist");

  /** The failures of all scripts, which the views of the scripts share */
  private static final class Store {

    /** Tables, views, functions and imports, with the namespace of their script */
    final Set<String> names = new HashSet<>();

    /**
     * Columns that failed to be added to a table, by their name only. The validator does not name
     * the table of a column that is not found, and the column is also missing from the tables that
     * select all columns of its table.
     */
    final Set<String> columns = new HashSet<>();

    /** Catalogs and databases, which do not belong to the namespace of a script */
    final Set<String> catalogs = new HashSet<>();

    /** The number of statements and imports recorded so far */
    int recorded = 0;

    boolean isEmpty() {
      return names.isEmpty() && columns.isEmpty() && catalogs.isEmpty();
    }
  }

  private final Store store;

  /** The namespace of the script's objects, or null for the main script */
  private final String namespace;

  /** Whether a USE statement of the script failed or was skipped */
  private boolean contextFailed = false;

  /** The failures as seen from the main script */
  FailedObjects() {
    this(new Store(), null);
  }

  private FailedObjects(Store store, String namespace) {
    this.store = store;
    this.namespace = namespace;
  }

  /**
   * The failures as seen from a script that is imported with a context of its own, i.e. not inline.
   * A script that is imported inline shares the view of the importing script.
   *
   * @param namespace the namespace of the script's objects, e.g. {@code sources} for {@code IMPORT
   *     package.sources}
   */
  FailedObjects forImportedScript(String namespace) {
    return new FailedObjects(store, namespace);
  }

  /**
   * Whether a USE statement of the script failed or was skipped, so that the errors of the
   * statements after it would be knock-on errors of running in another catalog or database than
   * intended.
   */
  boolean isContextFailed() {
    return contextFailed;
  }

  /**
   * The number of statements and imports recorded as failed or skipped so far, also those that
   * failed on an object that already failed. Comparing it before and after a statement tells
   * whether planning the statement recorded failures, e.g. in a script that it imports.
   */
  int recordedFailures() {
    return store.recorded;
  }

  /**
   * Whether the error of a statement is a knock-on error of an object that failed, i.e. that the
   * validator could not find a table, column or function that failed, or that a USE statement could
   * not find a catalog or database that failed.
   */
  boolean isKnockOnError(Throwable error) {
    if (store.isEmpty()) {
      return false;
    }
    return ExceptionUtils.getThrowableList(error).stream()
        .filter(
            cause -> cause instanceof SqlValidatorException || cause instanceof CatalogException)
        .map(Throwable::getMessage)
        .filter(Objects::nonNull)
        .anyMatch(this::isNotFoundOfFailed);
  }

  private boolean isNotFoundOfFailed(String message) {
    var object = OBJECT_NOT_FOUND.matcher(message);
    if (object.find()) {
      var name = object.group(1);
      if (object.group(2) != null) {
        // A qualified name that was not found within a catalog and database, e.g.
        // 'default_catalog.sources', whose database is the namespace of the script
        var qualified = lastSegment(object.group(2)) + "." + name;
        return isFailed(qualified, null) || isAllImported(qualified, null) || isCatalog(name);
      }
      return isFailed(name, namespace) || isAllImported(name, namespace) || isCatalog(name);
    }
    var column = COLUMN_NOT_FOUND.matcher(message);
    if (column.find()) {
      return store.columns.contains(normalize(column.group(1)));
    }
    var function = FUNCTION_NOT_FOUND.matcher(message);
    if (function.find()) {
      var name = function.group(1);
      return isFailed(name, namespace) || isAllImported(name, namespace);
    }
    var catalog = CATALOG_NOT_FOUND.matcher(message);
    return catalog.find() && isCatalog(catalog.group(1));
  }

  /**
   * Whether the statement adds to, exports or inserts into a table that failed, or uses one that
   * the planner looks up before it validates the statement. The planner would report that error, so
   * the statement is skipped instead.
   *
   * @param isDefined whether a table, as written in the statement (e.g. {@code sources.Orders}), is
   *     defined. A table that is defined does not belong to a failed {@code IMPORT package.*},
   *     whose objects are unknown.
   */
  boolean isDependent(StatementObjects statement, Predicate<List<String>> isDefined) {
    if (store.isEmpty()) {
      return false;
    }
    return statement.targets().stream().anyMatch(path -> isFailedPath(path, isDefined));
  }

  private boolean isFailedPath(List<String> path, Predicate<List<String>> isDefined) {
    var name = String.join(".", path);
    var pathNamespace = path.size() == 1 ? namespace : null;
    return isFailed(name, pathNamespace)
        || isCatalog(path.get(0))
        || (isAllImported(name, pathNamespace) && !isDefined.test(path));
  }

  /** Whether the object failed, or the imported script that it belongs to */
  private boolean isFailed(String name, String namespace) {
    var qualified = qualify(namespace, name);
    var parent = parent(qualified);
    return store.names.contains(qualified) || (parent != null && store.names.contains(parent));
  }

  /**
   * Whether an {@code IMPORT package.*} failed in the object's namespace, whose objects are
   * unknown, so that the object may be one of them
   */
  private boolean isAllImported(String name, String namespace) {
    var parent = parent(qualify(namespace, name));
    return store.names.contains(parent == null ? ALL : parent + "." + ALL);
  }

  private boolean isCatalog(String name) {
    return store.catalogs.contains(normalize(name));
  }

  /**
   * Records the objects of a statement that failed to parse or plan, or that was skipped because it
   * uses one that failed.
   */
  void record(StatementObjects statement) {
    store.recorded++;
    if (statement.isUse()) {
      contextFailed = true;
    }
    for (var defined : statement.defined()) {
      if (defined instanceof Named named) {
        var path = named.path();
        var name = path.get(path.size() - 1);
        add(namespace, name);
        if (path.size() > 1) {
          // The validator resolves a qualified name within its database, e.g. mydb.Orders
          add(null, path.get(path.size() - 2) + "." + name);
        }
      } else if (defined instanceof AllImported) {
        add(namespace, ALL);
      } else if (defined instanceof Column column) {
        store.columns.add(normalize(column.name()));
      } else if (defined instanceof Catalog catalog) {
        store.catalogs.add(normalize(catalog.name()));
      }
    }
  }

  /**
   * Records an object of a package that failed to import, e.g. a table of an {@code IMPORT
   * package.*}, whose other objects are still imported.
   */
  void recordImported(String name) {
    store.recorded++;
    add(namespace, name);
  }

  private void add(String namespace, String name) {
    if (!name.isBlank()) {
      store.names.add(qualify(namespace, name));
    }
  }

  private static String qualify(String namespace, String name) {
    return normalize(namespace == null ? name : namespace + "." + name);
  }

  private static String parent(String qualified) {
    var lastDot = qualified.lastIndexOf('.');
    return lastDot < 0 ? null : qualified.substring(0, lastDot);
  }

  private static String lastSegment(String path) {
    return path.substring(path.lastIndexOf('.') + 1);
  }

  private static String normalize(String name) {
    return name.toLowerCase(Locale.ROOT);
  }
}
