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

import com.datasqrl.planner.tables.SqrlTableFunction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Util;

/**
 * Resolves what the names in a validated query refer to, using the scopes of the validator that
 * validated it.
 */
@UtilityClass
public class QueryReferences {

  /**
   * A name in a query that refers to a table or to a column of a table.
   *
   * @param position the position of the name in the query; for a column the name may be qualified
   *     with the table or its alias, and columns that the validator expanded from {@code *} have
   *     the position of the {@code *}
   * @param table the qualified name of the table: catalog, database and table name
   * @param column the column and any nested field names, empty for a reference to the table
   * @param type the type of the column, if known
   */
  public record QueryReference(
      SqlParserPos position, List<String> table, List<String> column, Optional<RelDataType> type) {

    public boolean isColumn() {
      return !column.isEmpty();
    }
  }

  /**
   * Collects the references to tables and their columns in a query. Names that refer to anything
   * else, such as subqueries, aliases of the select list or functions, are skipped. A qualifier of
   * a column that is the name of its table, e.g. {@code Orders} in {@code Orders.id}, is a
   * reference to the table, and so is the name of a call of a table function of a script.
   *
   * @param query the query as returned by the validator
   * @param validator the validator that validated the query
   */
  public static List<QueryReference> collect(SqlNode query, SqlValidator validator) {
    var collector = new Collector(validator, explicitAliases(query), new ArrayList<>());
    collector.visit(query, null);
    return collector.references();
  }

  /**
   * The aliases that are written in a query, e.g. {@code o} in {@code FROM Orders o}, in lower
   * case. The validator gives tables without one an alias with the position of the table.
   */
  private static Set<String> explicitAliases(SqlNode query) {
    var aliases = new HashSet<String>();
    query.accept(
        new SqlBasicVisitor<Void>() {
          @Override
          public Void visit(SqlCall call) {
            if (call.getKind() == SqlKind.AS
                && call.operandCount() > 1
                && call.operand(1) instanceof SqlIdentifier alias
                && !alias.getParserPosition().equals(call.operand(0).getParserPosition())) {
              aliases.add(Util.last(alias.names).toLowerCase(Locale.ROOT));
            }
            return super.visit(call);
          }
        });
    return aliases;
  }

  private record Collector(
      SqlValidator validator, Set<String> explicitAliases, List<QueryReference> references) {

    void visit(SqlNode node, SqlValidatorScope scope) {
      if (node == null) {
        return;
      }
      if (node instanceof SqlIdentifier identifier) {
        visitIdentifier(identifier, scope);
      } else if (node instanceof SqlSelect select) {
        visitSelect(select);
      } else if (node instanceof SqlJoin join) {
        visit(join.getLeft(), scope);
        visit(join.getRight(), scope);
        visit(join.getCondition(), scopeOrDefault(() -> validator.getJoinScope(join), scope));
      } else if (node instanceof SqlNodeList list) {
        list.forEach(item -> visit(item, scope));
      } else if (node instanceof SqlCall call) {
        if (call.getKind() == SqlKind.AS) {
          // The other operands are the alias and its column names
          visit(call.operand(0), scope);
          return;
        }
        if (call.getOperator() instanceof SqlUserDefinedTableFunction function
            && function.getFunction() instanceof SqrlTableFunction sqrlFunction) {
          addFunctionCall(call, sqrlFunction);
        }
        call.getOperandList().forEach(operand -> visit(operand, scope));
      }
    }

    private void visitSelect(SqlSelect select) {
      var selectScope = scopeOrDefault(() -> validator.getSelectScope(select), null);
      visit(select.getFrom(), selectScope);
      visit(select.getSelectList(), selectScope);
      visit(select.getWhere(), scopeOrDefault(() -> validator.getWhereScope(select), selectScope));
      visit(select.getGroup(), scopeOrDefault(() -> validator.getGroupScope(select), selectScope));
      visit(
          select.getHaving(), scopeOrDefault(() -> validator.getHavingScope(select), selectScope));
      // The ORDER BY clause names columns of the select list, e.g. of `*`, which are resolved as
      // the columns of the tables of the query
      visitWithFallback(
          select.getOrderList(),
          scopeOrDefault(() -> validator.getOrderScope(select), selectScope),
          selectScope);
    }

    /** Visits names, and those that the scope does not resolve to columns with another scope. */
    private void visitWithFallback(
        SqlNode node, SqlValidatorScope scope, SqlValidatorScope fallback) {
      if (node instanceof SqlIdentifier identifier) {
        var found = references.size();
        visitIdentifier(identifier, scope);
        if (references.size() == found && fallback != null) {
          visitIdentifier(identifier, fallback);
        }
      } else if (node instanceof SqlNodeList list) {
        list.forEach(item -> visitWithFallback(item, scope, fallback));
      } else if (node instanceof SqlCall call && !(node instanceof SqlSelect)) {
        call.getOperandList().forEach(operand -> visitWithFallback(operand, scope, fallback));
      } else {
        visit(node, scope);
      }
    }

    private void visitIdentifier(SqlIdentifier identifier, SqlValidatorScope scope) {
      var position = identifier.getParserPosition();
      if (position.equals(SqlParserPos.ZERO)) {
        // The validator qualifies some names without a position, e.g. the nested field
        // `address.city` of a column as `T.address.city`, and remembers the name as written
        var original =
            validator instanceof SqlValidatorImpl impl ? impl.getOriginal(identifier) : identifier;
        if (original == identifier || original.getParserPosition().equals(SqlParserPos.ZERO)) {
          return; // added by a rewrite, not in the source
        }
        position = original.getParserPosition();
      }
      // Tables in FROM clauses are registered as namespaces
      var namespace = validator.getNamespace(identifier);
      if (namespace != null) {
        var table = namespace.getTable();
        if (table != null) {
          references.add(
              new QueryReference(position, table.getQualifiedName(), List.of(), Optional.empty()));
        }
        return;
      }
      if (scope == null || identifier.isStar()) {
        return;
      }
      // Calcite cannot resolve some qualified names that the validator expanded itself, e.g. in
      // the PARTITION BY clause of a window, so those are resolved without their qualifier
      for (var start = 0; start < identifier.names.size(); start++) {
        var qualified = qualify(identifier.getComponent(start, identifier.names.size()), scope);
        if (qualified.isPresent()) {
          var source = qualified.get().namespace;
          var table = source == null ? null : source.getTable();
          var suffix = qualified.get().suffix();
          if (table != null && !suffix.isEmpty()) {
            references.add(
                new QueryReference(
                    position, table.getQualifiedName(), List.copyOf(suffix), typeOf(identifier)));
            addTableQualifier(
                identifier,
                start,
                identifier.names.size() - suffix.size(),
                table.getQualifiedName());
          }
          return; // a column of a table, or e.g. of a subquery
        }
      }
    }

    /**
     * Adds a reference to a table function for the name of a call, e.g. {@code Fn} in {@code
     * Fn(1)}.
     */
    private void addFunctionCall(SqlCall call, SqrlTableFunction function) {
      var position = call.getParserPosition();
      if (position.equals(SqlParserPos.ZERO)) {
        return;
      }
      var identifier = function.getFunctionAnalysis().getObjectIdentifier();
      var length = identifier.getObjectName().length();
      references.add(
          new QueryReference(
              new SqlParserPos(
                  position.getLineNum(),
                  position.getColumnNum(),
                  position.getLineNum(),
                  position.getColumnNum() + length - 1),
              identifier.toList(),
              List.of(),
              Optional.empty()));
    }

    /**
     * Adds a reference to the table for the qualifier of a column between the given components, if
     * the qualifier is the name of the table and not an alias.
     */
    private void addTableQualifier(
        SqlIdentifier identifier, int start, int end, List<String> table) {
      if (end <= start) {
        return;
      }
      var name = identifier.names.get(end - 1);
      if (!name.equalsIgnoreCase(Util.last(table))
          || explicitAliases.contains(name.toLowerCase(Locale.ROOT))) {
        return;
      }
      var position = identifier.getComponentParserPosition(start);
      for (var i = start; i < end; i++) {
        var component = identifier.getComponentParserPosition(i);
        if (component.equals(SqlParserPos.ZERO)
            || component.equals(identifier.getParserPosition())) {
          return; // added by the validator, or without a position of its own
        }
        position = position.plus(component);
      }
      references.add(new QueryReference(position, table, List.of(), Optional.empty()));
    }

    private Optional<RelDataType> typeOf(SqlIdentifier identifier) {
      try {
        return Optional.ofNullable(validator.getValidatedNodeTypeIfKnown(identifier));
      } catch (RuntimeException e) {
        return Optional.empty();
      }
    }

    private static Optional<SqlQualified> qualify(
        SqlIdentifier identifier, SqlValidatorScope scope) {
      try {
        return Optional.of(scope.fullyQualify(identifier));
      } catch (RuntimeException e) {
        // Not a column, e.g. an alias of the select list in the ORDER BY clause
        return Optional.empty();
      }
    }

    private static SqlValidatorScope scopeOrDefault(
        Supplier<SqlValidatorScope> scope, SqlValidatorScope defaultScope) {
      try {
        var result = scope.get();
        return result == null ? defaultScope : result;
      } catch (RuntimeException e) {
        return defaultScope;
      }
    }
  }
}
