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

import static java.util.stream.Collectors.joining;

import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
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
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.flink.sql.parser.ddl.view.SqlAlterViewAs;
import org.apache.flink.sql.parser.ddl.view.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.utils.EncodingUtils;

/**
 * Expands a bare relation alias in a SELECT list into a ROW value over the columns of that
 * relation, which neither Calcite nor Flink support natively.
 *
 * <p>For example, {@code SELECT p AS project FROM Projects p} is rewritten to {@code SELECT
 * CAST(ROW(p.id, p.name) AS ROW<`id` BIGINT, `name` STRING>) AS project FROM Projects p} so that a
 * table's entire row can be nested under a single field without re-listing its columns.
 *
 * <p>A column of the same name takes precedence over the relation alias, and any query that cannot
 * be resolved is left untouched so that the regular validation reports the error.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class RelationAliasExpander {

  private final Sqrl2FlinkSQLTranslator translator;

  /** Rewrites the given statement in place and returns it. */
  SqlNode expand(SqlNode statement) {
    var query = getQuery(statement);
    if (query != null) {
      query.accept(
          new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
              var result = super.visit(call);
              if (result instanceof SqlSelect select) {
                expandSelect(select);
              }
              return result;
            }
          });
    }
    return statement;
  }

  private @Nullable SqlNode getQuery(SqlNode statement) {
    if (statement instanceof SqlCreateView view) {
      return view.getQuery();
    }
    if (statement instanceof SqlAlterViewAs alterView) {
      return alterView.getNewQuery();
    }
    if (statement instanceof RichSqlInsert insert) {
      return insert.getSource();
    }
    return statement.getKind() == SqlKind.WITH || SqlKind.QUERY.contains(statement.getKind())
        ? statement
        : null;
  }

  private void expandSelect(SqlSelect select) {
    var from = select.getFrom();
    if (from == null) {
      return;
    }
    var relationAliases = new HashSet<String>();
    collectAliases(from, relationAliases);
    var selectList = select.getSelectList();
    var candidates = new LinkedHashMap<Integer, String>();
    for (var i = 0; i < selectList.size(); i++) {
      var alias = getBareIdentifier(selectList.get(i));
      if (alias != null && relationAliases.contains(alias)) {
        candidates.put(i, alias);
      }
    }
    if (candidates.isEmpty()) {
      return;
    }

    var probe = createProbe(select);
    var fromType = resolveRowType(probe, SqlIdentifier.star(SqlParserPos.ZERO));
    if (fromType == null) {
      return;
    }
    var columns = Set.copyOf(fromType.getFieldNames());

    var items = new ArrayList<>(selectList.getList());
    var expanded = false;
    for (var candidate : candidates.entrySet()) {
      var alias = candidate.getValue();
      if (columns.contains(alias)) {
        continue;
      }
      var star = new SqlIdentifier(alias, SqlParserPos.ZERO).plusStar();
      var rowType = resolveRowType(probe, star);
      if (rowType == null || rowType.getFieldCount() == 0) {
        continue;
      }
      var rowValue = createRowValue(alias, rowType);
      if (rowValue == null) {
        continue;
      }
      var item = items.get(candidate.getKey());
      items.set(
          candidate.getKey(), SqlValidatorUtil.addAlias(rowValue, SqlValidatorUtil.alias(item)));
      expanded = true;
    }
    if (expanded) {
      select.setSelectList(new SqlNodeList(items, selectList.getParserPosition()));
    }
  }

  /**
   * Creates a copy of the query that keeps only the FROM clause, so that the columns each relation
   * alias contributes can be resolved by validating it on its own.
   */
  private SqlSelect createProbe(SqlSelect select) {
    var probe = (SqlSelect) select.clone(SqlParserPos.ZERO);
    probe.setWhere(null);
    probe.setGroupBy(null);
    probe.setHaving(null);
    probe.setQualify(null);
    probe.setOrderBy(null);
    probe.setOffset(null);
    probe.setFetch(null);
    return probe;
  }

  private void collectAliases(SqlNode from, Set<String> aliases) {
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

  private @Nullable String getBareIdentifier(SqlNode selectItem) {
    SqlNode node =
        selectItem.getKind() == SqlKind.AS ? ((SqlCall) selectItem).operand(0) : selectItem;
    return node instanceof SqlIdentifier identifier
            && identifier.names.size() == 1
            && !identifier.isStar()
        ? identifier.getSimple()
        : null;
  }

  /**
   * Validates the probe query for the given select list item. The query is re-parsed from its
   * serialized form because validation modifies the {@link SqlNode} tree it is given.
   */
  private @Nullable RelDataType resolveRowType(SqlSelect probe, SqlIdentifier star) {
    probe.setSelectList(new SqlNodeList(List.of(star), SqlParserPos.ZERO));
    var query = RelToFlinkSql.convertToString(probe);
    try {
      return translator.validateRowType(translator.parseSQL(query));
    } catch (Exception e) {
      log.debug("Could not resolve the row type of [{}]", query, e);
      return null;
    }
  }

  private @Nullable SqlNode createRowValue(String alias, RelDataType rowType) {
    var quotedAlias = quote(alias);
    var fields =
        rowType.getFieldNames().stream()
            .map(field -> quotedAlias + "." + quote(field))
            .collect(joining(", "));
    try {
      var rowTypeSql = FlinkTypeFactory.toLogicalType(rowType).copy(true).asSerializableString();
      var query =
          (SqlSelect)
              translator.parseSQL("SELECT CAST(ROW(%s) AS %s)".formatted(fields, rowTypeSql));
      return query.getSelectList().get(0);
    } catch (Exception e) {
      log.debug("Could not create a row value for relation alias [{}]", alias, e);
      return null;
    }
  }

  private static String quote(String identifier) {
    return EncodingUtils.escapeIdentifier(identifier);
  }
}
