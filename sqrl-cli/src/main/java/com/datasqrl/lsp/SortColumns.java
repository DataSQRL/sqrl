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

import com.datasqrl.compile.check.ScriptText;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.TreeSet;
import lombok.experimental.UtilityClass;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

/**
 * Finds the sort columns that the SELECT clause of a table definition is missing, which a table
 * definition has to select. Only columns that are sorted by name are found.
 */
@UtilityClass
class SortColumns {

  /**
   * The columns to add to a SELECT clause.
   *
   * @param columns the sort keys of the columns as written, e.g. {@code p.amount}
   * @param offset where the SELECT clause ends in the script
   */
  record Selection(String columns, int offset) {}

  private static final SqlParser.Config SQL_PARSER =
      SqlParser.config()
          .withParserFactory(FlinkSqlParserImpl.FACTORY)
          .withConformance(FlinkSqlConformance.DEFAULT)
          .withLex(Lex.JAVA)
          .withIdentifierMaxLength(256);

  /**
   * Finds the missing sort columns of the table definition at an offset.
   *
   * @param script the script
   * @param offset an offset in the statement of the table definition
   * @param missingNames the names of the missing columns
   */
  static Optional<Selection> find(ScriptText script, int offset, Collection<String> missingNames) {
    var missing = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    missing.addAll(missingNames);
    var end = CompletionContext.statementEnd(script.text(), offset);
    var context = CompletionContext.at(script.text(), end);
    if (context.kind() != CompletionContext.Kind.QUERY) {
      return Optional.empty();
    }
    SqlNode query;
    try {
      query = SqlParser.create(context.sql(), SQL_PARSER).parseQuery();
    } catch (SqlParseException e) {
      return Optional.empty();
    }
    if (!(query instanceof SqlOrderBy orderBy) || !(orderBy.query instanceof SqlSelect select)) {
      return Optional.empty();
    }
    // The positions of the parser are in the query, which has the same offsets as the script
    var sqlStart = end - context.sqlCursor();
    var sql = new ScriptText(context.sql());
    var columns = new ArrayList<String>();
    for (var item : orderBy.orderList) {
      if (sortKey(item) instanceof SqlIdentifier key && missing.remove(Util.last(key.names))) {
        var position = key.getParserPosition();
        columns.add(
            script
                .text()
                .substring(sqlStart + start(sql, position), sqlStart + end(sql, position)));
      }
    }
    if (!missing.isEmpty() || columns.isEmpty()) {
      return Optional.empty();
    }
    var lastItem = select.getSelectList().get(select.getSelectList().size() - 1);
    return Optional.of(
        new Selection(
            String.join(", ", columns), sqlStart + end(sql, lastItem.getParserPosition())));
  }

  /** The expression of a sort key, without its direction and the ordering of nulls. */
  private static SqlNode sortKey(SqlNode item) {
    while (item instanceof SqlCall call
        && (call.getKind() == SqlKind.DESCENDING
            || call.getKind() == SqlKind.NULLS_FIRST
            || call.getKind() == SqlKind.NULLS_LAST)) {
      item = call.operand(0);
    }
    return item;
  }

  private static int start(ScriptText sql, SqlParserPos position) {
    return sql.offset(position.getLineNum() - 1, position.getColumnNum() - 1);
  }

  private static int end(ScriptText sql, SqlParserPos position) {
    return sql.offset(position.getEndLineNum() - 1, position.getEndColumnNum() - 1) + 1;
  }
}
