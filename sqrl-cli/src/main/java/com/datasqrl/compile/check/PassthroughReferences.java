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

import com.datasqrl.compile.check.SqlTokens.Element;
import com.datasqrl.compile.check.SqlTokens.Kind;
import com.datasqrl.compile.check.SymbolIndex.Column;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Resolves the tables and columns that the SQL of a pass-through table function names. That SQL is
 * passed to the database as it is, in its dialect, e.g. with double-quoted names, so the planner
 * neither parses nor validates it. Instead, the SQL is split into names and the queries they are
 * in: the names after FROM and JOIN that are tables of the script are references to them, and a
 * name is a reference to a column if a table of its query has that column. Names that refer to
 * anything else, e.g. common table expressions and their columns, are left out.
 */
final class PassthroughReferences {

  /** Unquoted names that are part of the syntax, which are not columns */
  private static final Set<String> KEYWORDS =
      Set.of(
          "all",
          "and",
          "any",
          "array",
          "as",
          "asc",
          "between",
          "by",
          "case",
          "cast",
          "cross",
          "current",
          "default",
          "desc",
          "distinct",
          "else",
          "end",
          "except",
          "exists",
          "false",
          "fetch",
          "filter",
          "first",
          "following",
          "from",
          "full",
          "group",
          "having",
          "ilike",
          "in",
          "inner",
          "intersect",
          "interval",
          "is",
          "join",
          "lateral",
          "left",
          "like",
          "limit",
          "natural",
          "next",
          "not",
          "null",
          "nulls",
          "offset",
          "on",
          "only",
          "or",
          "order",
          "outer",
          "over",
          "partition",
          "preceding",
          "qualify",
          "range",
          "recursive",
          "returning",
          "right",
          "row",
          "rows",
          "select",
          "similar",
          "some",
          "then",
          "true",
          "unbounded",
          "union",
          "using",
          "values",
          "when",
          "where",
          "window",
          "with",
          "within");

  /** The keywords that end the FROM clause of a query */
  private static final Set<String> AFTER_FROM =
      Set.of(
          "where",
          "group",
          "having",
          "order",
          "limit",
          "offset",
          "fetch",
          "window",
          "qualify",
          "union",
          "intersect",
          "except",
          "returning");

  /** The keywords that end a query at its level */
  private static final Set<String> SET_OPERATORS = Set.of("union", "intersect", "except");

  /** The keywords that can follow a table in the FROM clause instead of an alias */
  private static final Set<String> NOT_ALIASES =
      Set.of(
          "on",
          "using",
          "join",
          "inner",
          "left",
          "right",
          "full",
          "cross",
          "natural",
          "outer",
          "lateral",
          "where",
          "group",
          "having",
          "order",
          "limit",
          "offset",
          "fetch",
          "window",
          "qualify",
          "union",
          "intersect",
          "except",
          "returning",
          "for",
          "tablesample");

  /**
   * A table that the SQL can name.
   *
   * @param identifier the table
   * @param columns its columns
   */
  record Table(ObjectIdentifier identifier, List<Column> columns) {

    Optional<Column> column(List<String> path) {
      var column = Column.named(columns, path.get(0));
      for (var i = 1; i < path.size() && column.isPresent(); i++) {
        column = column.get().field(path.get(i));
      }
      return column;
    }
  }

  /**
   * A name that refers to a table or column.
   *
   * @param start the offset of the name in the SQL; for a column the name may be qualified
   * @param end the offset after the name
   * @param table the table
   * @param column the column and its nested fields, empty for a reference to the table
   * @param type the type of the column
   */
  record Found(
      int start, int end, ObjectIdentifier table, List<String> column, Optional<String> type) {}

  /**
   * A query, from its SELECT to its end, and what the names in its FROM clause refer to.
   *
   * @param start the index of its first element
   * @param end the index after its last element
   * @param depth how many parentheses it is in
   * @param parent the query it is in, or null
   */
  private record Query(
      int start, int end, int depth, Query parent, Map<String, Optional<Table>> sources) {}

  private final List<Element> elements;
  private final int[] depths;
  private final Map<String, Table> tables = new HashMap<>();
  private final Optional<Table> parent;
  private final Set<String> commonTables = new HashSet<>();
  private final Set<Integer> consumed = new HashSet<>();
  private final List<Query> queries = new ArrayList<>();
  private final List<Found> found = new ArrayList<>();

  private PassthroughReferences(String sql, List<Table> tables, Optional<Table> parent) {
    this.elements = SqlTokens.tokenize(sql);
    this.depths = new int[elements.size()];
    var depth = 0;
    for (var i = 0; i < elements.size(); i++) {
      if (elements.get(i).isPunctuation(")")) {
        depth = Math.max(0, depth - 1);
      }
      depths[i] = depth;
      if (elements.get(i).isPunctuation("(")) {
        depth++;
      }
    }
    tables.forEach(
        table ->
            this.tables.putIfAbsent(
                table.identifier().getObjectName().toLowerCase(Locale.ROOT), table));
    this.parent = parent;
  }

  /**
   * Resolves the names in the SQL of a pass-through function.
   *
   * @param sql the SQL as it is written, with the parameters of the function
   * @param tables the tables that the SQL can name
   * @param parent the table of a relationship, whose columns {@code this.} refers to
   */
  static List<Found> resolve(String sql, List<Table> tables, Optional<Table> parent) {
    var references = new PassthroughReferences(sql, tables, parent);
    references.findCommonTables();
    references.findQueries();
    references.queries.forEach(references::resolveFrom);
    references.resolveColumns();
    return List.copyOf(references.found);
  }

  /** The names of common table expressions, which are not tables of the script. */
  private void findCommonTables() {
    for (var i = 0; i + 2 < elements.size(); i++) {
      var element = elements.get(i);
      if (element.kind() == Kind.NAME
          && element.parts().size() == 1
          && (i > 0
              && (elements.get(i - 1).is("with")
                  || elements.get(i - 1).is("recursive")
                  || elements.get(i - 1).isPunctuation(",")))
          && (elements.get(i + 1).is("as") || elements.get(i + 1).isPunctuation("("))) {
        commonTables.add(element.parts().get(0).toLowerCase(Locale.ROOT));
      }
    }
  }

  /** Finds the queries, i.e. the SELECTs and where they end. */
  private void findQueries() {
    for (var i = 0; i < elements.size(); i++) {
      if (!elements.get(i).is("select")) {
        continue;
      }
      var depth = depths[i];
      var end = i + 1;
      while (end < elements.size()) {
        var element = elements.get(end);
        // The parenthesis that closes the query has the depth outside of it
        if (depths[end] < depth
            || (depths[end] == depth
                && (element.isPunctuation(";") || element.isAny(SET_OPERATORS)))) {
          break;
        }
        end++;
      }
      Query enclosing = null;
      for (var query : queries) {
        if (query.start() < i && i < query.end() && query.depth() < depth) {
          enclosing = enclosing == null || query.depth() > enclosing.depth() ? query : enclosing;
        }
      }
      queries.add(new Query(i, end, depth, enclosing, new HashMap<>()));
    }
  }

  /** Resolves the tables in the FROM clause of a query, and their aliases. */
  private void resolveFrom(Query query) {
    var i = query.start();
    while (i < query.end() && !(depths[i] == query.depth() && elements.get(i).is("from"))) {
      i++;
    }
    var expectTable = true;
    for (i++; i < query.end(); i++) {
      var element = elements.get(i);
      if (depths[i] != query.depth()) {
        continue;
      }
      if (element.isAny(AFTER_FROM)) {
        return;
      }
      if (element.isPunctuation(",") || element.is("join")) {
        expectTable = true;
      } else if (expectTable && element.is("lateral")) {
        continue;
      } else if (expectTable && element.isPunctuation("(")) {
        // A subquery, whose columns are not known
        i = skipParentheses(i);
        i = addAlias(query, i + 1, Optional.empty(), null) - 1;
        expectTable = false;
      } else if (expectTable && element.kind() == Kind.NAME) {
        var name = element.parts().get(element.parts().size() - 1).toLowerCase(Locale.ROOT);
        var table =
            commonTables.contains(name)
                ? Optional.<Table>empty()
                : Optional.ofNullable(tables.get(name));
        consumed.add(i);
        table.ifPresent(
            t ->
                found.add(
                    new Found(
                        element.start(),
                        element.end(),
                        t.identifier(),
                        List.of(),
                        Optional.empty())));
        var next = i + 1;
        if (next < query.end() && elements.get(next).isPunctuation("(")) {
          next = skipParentheses(next) + 1; // a function call
        }
        i = addAlias(query, next, table, name) - 1;
        expectTable = false;
      }
    }
  }

  /**
   * Adds the alias of a table in the FROM clause, or its name if it has no alias.
   *
   * @return the index after the alias
   */
  private int addAlias(Query query, int index, Optional<Table> table, String name) {
    var i = index;
    if (i < query.end() && elements.get(i).is("as")) {
      i++;
    }
    if (i < query.end()
        && elements.get(i).kind() == Kind.NAME
        && elements.get(i).parts().size() == 1
        && !elements.get(i).isAny(NOT_ALIASES)) {
      consumed.add(i);
      query.sources().put(elements.get(i).parts().get(0).toLowerCase(Locale.ROOT), table);
      return i + 1;
    }
    if (name != null) {
      query.sources().put(name, table);
    }
    return index;
  }

  /** Resolves the names that are not tables or aliases as columns of the tables of their query. */
  private void resolveColumns() {
    for (var i = 0; i < elements.size(); i++) {
      var element = elements.get(i);
      if (element.kind() != Kind.NAME
          || consumed.contains(i)
          || element.isAny(KEYWORDS)
          || (i > 0 && elements.get(i - 1).is("as"))
          || (i + 1 < elements.size()
              && (elements.get(i + 1).isPunctuation("(")
                  || elements.get(i + 1).text().startsWith("'")))) {
        continue; // a keyword, an alias of a column, a function, or e.g. the type of DATE '...'
      }
      var query = queryAt(i);
      var parts = element.parts();
      Optional<Table> table = Optional.empty();
      List<String> path = parts;
      if (parts.size() > 1 && parts.get(0).equalsIgnoreCase("this") && parent.isPresent()) {
        table = parent;
        path = parts.subList(1, parts.size());
      } else if (parts.size() > 1) {
        var source = source(query, parts.get(0));
        if (source != null) {
          table = source;
          path = parts.subList(1, parts.size());
        }
      }
      if (table.isEmpty() && (parts.size() == 1 || source(query, parts.get(0)) == null)) {
        table = tableWithColumn(query, parts);
        path = parts;
      }
      var columnPath = path;
      var column = table.flatMap(t -> t.column(columnPath));
      if (column.isPresent()) {
        found.add(
            new Found(
                element.start(),
                element.end(),
                table.get().identifier(),
                List.copyOf(columnPath),
                column.map(Column::type)));
      }
    }
  }

  /** What a name in the FROM clause of a query or the queries it is in refers to, or null. */
  private static Optional<Table> source(Query query, String name) {
    for (var q = query; q != null; q = q.parent()) {
      var source = q.sources().get(name.toLowerCase(Locale.ROOT));
      if (source != null) {
        return source;
      }
    }
    return null;
  }

  /** The only table of the innermost query that has a column, e.g. of an unqualified name. */
  private static Optional<Table> tableWithColumn(Query query, List<String> path) {
    for (var q = query; q != null; q = q.parent()) {
      var candidates =
          q.sources().values().stream()
              .flatMap(Optional::stream)
              .distinct()
              .filter(t -> t.column(path).isPresent())
              .toList();
      if (candidates.size() == 1) {
        return Optional.of(candidates.get(0));
      } else if (candidates.size() > 1) {
        return Optional.empty(); // ambiguous
      }
    }
    return Optional.empty();
  }

  /** The innermost query that contains an element. */
  private Query queryAt(int index) {
    Query result = null;
    for (var query : queries) {
      if (query.start() <= index
          && index < query.end()
          && depths[index] >= query.depth()
          && (result == null || query.depth() >= result.depth())) {
        result = query;
      }
    }
    return result;
  }

  /** The index of the parenthesis that closes the one at an index. */
  private int skipParentheses(int open) {
    var depth = depths[open];
    var i = open + 1;
    while (i < elements.size() && !(elements.get(i).isPunctuation(")") && depths[i] == depth)) {
      i++;
    }
    return Math.min(i, elements.size() - 1);
  }
}
