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

import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.validate.SqlMonikerType;

/**
 * The tables and functions that a check planned, kept open so that queries can be completed against
 * them. It is safe to use from several threads, and must be closed when it is no longer needed.
 */
@Slf4j
public class PlannedCatalog implements AutoCloseable {

  /**
   * A suggested name.
   *
   * @param type what it names, e.g. a table or a column
   * @param names the qualified name, e.g. catalog, database and table name of a table
   */
  public record Suggestion(SqlMonikerType type, List<String> names) {

    public String name() {
      return names.get(names.size() - 1);
    }
  }

  /**
   * The suggestions for a position in a query.
   *
   * @param suggestions the suggested names
   * @param replaced the text before the position that the suggestions replace
   */
  public record Completions(List<Suggestion> suggestions, String replaced) {

    public static final Completions NONE = new Completions(List.of(), "");
  }

  private final Sqrl2FlinkSQLTranslator environment;
  private List<String> functionNames;
  private boolean closed = false;

  PlannedCatalog(Sqrl2FlinkSQLTranslator environment) {
    this.environment = environment;
  }

  /**
   * Suggests names to complete a query with at a position.
   *
   * @param sql the query
   * @param cursor the position in the query
   * @param database the database that unqualified table names refer to
   */
  public synchronized Completions complete(String sql, int cursor, String database) {
    if (closed) {
      return Completions.NONE;
    }
    var catalogs = environment.getCatalogManager();
    var previousDatabase = catalogs.getCurrentDatabase();
    try {
      if (!database.equals(previousDatabase)
          && catalogs
              .getCatalog(catalogs.getCurrentCatalog())
              .map(catalog -> catalog.databaseExists(database))
              .orElse(false)) {
        catalogs.setCurrentDatabase(database);
      }
      var replaced = new String[1];
      var suggestions =
          environment.getCompletionHints(sql, cursor, replaced).stream()
              .map(hint -> new Suggestion(hint.getType(), hint.getFullyQualifiedNames()))
              .toList();
      return new Completions(suggestions, replaced[0] == null ? "" : replaced[0]);
    } catch (RuntimeException e) {
      log.debug("Could not complete {}", sql, e);
      return Completions.NONE;
    } finally {
      catalogs.setCurrentDatabase(previousDatabase);
    }
  }

  /** The names of all functions, built-in and user-defined. */
  public synchronized List<String> functionNames() {
    if (closed) {
      return List.of();
    }
    if (functionNames == null) {
      functionNames = environment.listFunctions();
    }
    return functionNames;
  }

  /**
   * The signatures of a function, built-in or user-defined, each as the descriptions of its
   * parameters, e.g. {@code [<CHARACTER_STRING>, <INTEGER_NUMERIC>]}.
   */
  public synchronized List<List<String>> functionSignatures(String name) {
    if (closed) {
      return List.of();
    }
    try {
      return environment.functionSignatures(name);
    } catch (RuntimeException e) {
      log.debug("Could not find the signatures of {}", name, e);
      return List.of();
    }
  }

  /** The class of a user-defined function, e.g. {@code com.myudf.MyScalarFunction}. */
  public synchronized Optional<String> functionClass(String name) {
    if (closed) {
      return Optional.empty();
    }
    try {
      return environment.functionClass(name);
    } catch (RuntimeException e) {
      log.debug("Could not find the class of {}", name, e);
      return Optional.empty();
    }
  }

  /** Releases the catalog, after any completion that is running on another thread. */
  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      environment.close();
    }
  }
}
