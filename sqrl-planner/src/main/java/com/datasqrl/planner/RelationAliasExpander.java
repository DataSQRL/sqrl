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

import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.engine.stream.flink.FlinkCalciteParser;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;

/**
 * Expands a bare relation alias in a SELECT list into a ROW value over the columns of that
 * relation, which neither Calcite nor Flink support natively.
 *
 * <p>For example, {@code SELECT p AS project FROM Projects p} is rewritten to {@code SELECT
 * CAST(ROW(p.id, p.name) AS ROW(id BIGINT, name VARCHAR)) AS project FROM Projects p} so that a
 * table's entire row can be nested under a single field without re-listing its columns.
 *
 * <p>The columns an alias contributes are resolved by validating a probe query built from the FROM
 * clause alone, wrapped back into the {@code WITH} bindings visible at that point so that CTEs stay
 * in scope. A column of the same name takes precedence over the relation alias. A FROM clause that
 * does not validate on its own - a correlated or LATERAL join referencing the enclosing query - is
 * left untouched, so that the regular validation reports the error.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class RelationAliasExpander {

  private final TableEnvironmentImpl tEnv;
  private final Supplier<FlinkPlannerImpl> plannerSupplier;

  /** Rewrites the given statement in place and returns it. */
  SqlNode expand(SqlNode statement) {
    statement.accept(new SelectListVisitor());
    return statement;
  }

  /**
   * Tracks the CTE bindings visible at each SELECT so probes can be built in their scope. A CTE
   * definition only sees the bindings declared before it, the body sees all of them.
   */
  private class SelectListVisitor extends SqlBasicVisitor<Void> {

    private final Deque<SqlNodeList> visibleWithLists = new ArrayDeque<>();

    @Override
    public Void visit(SqlCall call) {
      if (call instanceof SqlWith with) {
        var visible = new SqlNodeList(SqlParserPos.ZERO);
        visibleWithLists.push(visible);
        for (SqlNode item : with.withList) {
          item.accept(this);
          visible.add(item);
        }
        with.body.accept(this);
        visibleWithLists.pop();
        return null;
      }
      super.visit(call);
      if (call instanceof SqlSelect select) {
        expandSelect(select, visibleWithLists);
      }
      return null;
    }
  }

  private void expandSelect(SqlSelect select, Deque<SqlNodeList> visibleWithLists) {
    if (select.getFrom() == null) {
      return;
    }
    var relationAliases = new HashSet<String>();
    collectAliases(select.getFrom(), relationAliases);
    var selectList = select.getSelectList();
    if (selectList.stream().noneMatch(item -> relationAliases.contains(bareName(item)))) {
      return;
    }

    var queryType = resolveType(SqlIdentifier.star(SqlParserPos.ZERO), select, visibleWithLists);
    if (queryType == null) {
      return;
    }
    var columns = Set.copyOf(queryType.getFieldNames());

    for (var i = 0; i < selectList.size(); i++) {
      var item = selectList.get(i);
      var alias = bareName(item);
      if (!relationAliases.contains(alias) || columns.contains(alias)) {
        continue;
      }
      var star = new SqlIdentifier(alias, SqlParserPos.ZERO).plusStar();
      var rowType = resolveType(star, select, visibleWithLists);
      if (rowType != null) {
        selectList.set(
            i, SqlValidatorUtil.addAlias(rowValue(alias, rowType), SqlValidatorUtil.alias(item)));
      }
    }
  }

  /**
   * Validates a copy of the query that selects only the given item and keeps only the FROM clause,
   * and returns the row type it produces. The probe is re-parsed from its serialized form because
   * validation modifies the {@link SqlNode} tree it is given.
   */
  private @Nullable RelDataType resolveType(
      SqlIdentifier item, SqlSelect select, Deque<SqlNodeList> visibleWithLists) {
    var probe = (SqlSelect) select.clone(SqlParserPos.ZERO);
    probe.setSelectList(new SqlNodeList(List.of(item), SqlParserPos.ZERO));
    probe.setWhere(null);
    probe.setGroupBy(null);
    probe.setHaving(null);
    probe.setQualify(null);
    probe.setOrderBy(null);
    probe.setOffset(null);
    probe.setFetch(null);

    SqlNode statement = probe;
    for (SqlNodeList withList : visibleWithLists) {
      if (!withList.isEmpty()) {
        statement = new SqlWith(SqlParserPos.ZERO, withList, statement);
      }
    }

    var query = RelToFlinkSql.convertToString(statement);
    try {
      var planner = plannerSupplier.get();
      var validated = planner.validate(FlinkCalciteParser.parseSql(query, tEnv));
      return planner.getOrCreateSqlValidator().getValidatedNodeType(validated);
    } catch (Exception e) {
      log.debug("Could not resolve the row type of [{}]", query, e);
      return null;
    }
  }

  private static SqlNode rowValue(String alias, RelDataType rowType) {
    var fields =
        rowType.getFieldNames().stream()
            .map(field -> (SqlNode) new SqlIdentifier(List.of(alias, field), SqlParserPos.ZERO))
            .toList();
    return SqlStdOperatorTable.CAST.createCall(
        SqlParserPos.ZERO,
        SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, fields),
        SqlDataTypeSpecBuilder.convertTypeToSpec(rowType));
  }

  private static void collectAliases(SqlNode from, Set<String> aliases) {
    if (from instanceof SqlJoin join) {
      collectAliases(join.getLeft(), aliases);
      collectAliases(join.getRight(), aliases);
      return;
    }
    var alias = SqlValidatorUtil.alias(from);
    if (alias != null) {
      aliases.add(alias);
    }
  }

  private static @Nullable String bareName(SqlNode selectItem) {
    SqlNode node =
        selectItem.getKind() == SqlKind.AS ? ((SqlCall) selectItem).operand(0) : selectItem;
    return node instanceof SqlIdentifier identifier
            && identifier.names.size() == 1
            && !identifier.isStar()
        ? identifier.getSimple()
        : null;
  }
}
