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
package org.apache.calcite.rel.rel2sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

/**
 * Make the following changes to the generic RelToSqlConverter: - uses only the objectname for table
 * scans (i.e. remove catalog and database identifiers) - add hints to generated sql - other sqrl
 * specific changes (TODO: what are those? are those still needed?)
 */
public class RelToSqlConverterWithHints extends org.apache.calcite.rel.rel2sql.RelToSqlConverter {

  public RelToSqlConverterWithHints(SqlDialect dialect) {
    super(dialect);
  }

  // SQRL: add hints
  @Override
  public SqlImplementor.Result visit(Project e) {
    var x = this.visitInput(e, 0, new SqlImplementor.Clause[] {Clause.SELECT});
    this.parseCorrelTable(e, x);
    var builder = x.builder(e);
    if (!isStar(e.getProjects(), e.getInput().getRowType(), e.getRowType())) {
      List<SqlNode> selectList = new ArrayList();

      SqlNode sqlExpr;
      for (Iterator var5 = e.getProjects().iterator();
          var5.hasNext();
          this.addSelect(selectList, sqlExpr, e.getRowType())) {
        var ref = (RexNode) var5.next();
        sqlExpr = builder.context.toSql((RexProgram) null, ref);
        if (SqlUtil.isNullLiteral(sqlExpr, false)) {
          var field = e.getRowType().getFieldList().get(selectList.size());
          sqlExpr = this.castNullType(sqlExpr, field.getType());
        }
      }

      builder.setSelect(new SqlNodeList(selectList, POS));
    }

    List<SqlNode> hints =
        e.getHints().stream()
            .filter(h -> h.inheritPath.size() == 0)
            .map(
                h ->
                    new SqlHint(
                        SqlParserPos.ZERO,
                        new SqlIdentifier(h.hintName, SqlParserPos.ZERO),
                        new SqlNodeList(
                            h.listOptions.stream()
                                .map(s -> new SqlIdentifier(s, SqlParserPos.ZERO))
                                .collect(Collectors.toList()),
                            SqlParserPos.ZERO),
                        HintOptionFormat.ID_LIST))
            .collect(Collectors.toList());
    builder.select.setHints(new SqlNodeList(hints, SqlParserPos.ZERO));

    return builder.result();
  }

  private SqlNode castNullType(SqlNode nullLiteral, RelDataType type) {
    SqlNode typeNode = this.dialect.getCastSpec(type);
    return typeNode == null
        ? nullLiteral
        : SqlStdOperatorTable.CAST.createCall(POS, new SqlNode[] {nullLiteral, typeNode});
  }

  /** SQRL: Preserve join type & change in field aliasing (calcite bug?) */
  @Override
  public SqlImplementor.Result visit(Correlate e) {
    // sqrl: change e.getRowType() to e.getInput(0).getRowType()
    var leftResult =
        this.visitInput(e, 0).resetAlias(e.getCorrelVariable(), e.getInput(0).getRowType());
    parseCorrelTable(e, leftResult);
    var rightResult = this.visitInput(e, 1);
    SqlNode rightLateral =
        SqlStdOperatorTable.LATERAL.createCall(POS, new SqlNode[] {rightResult.node});
    SqlNode rightLateralAs =
        SqlStdOperatorTable.AS.createCall(
            POS, new SqlNode[] {rightLateral, new SqlIdentifier(rightResult.neededAlias, POS)});
    SqlNode join =
        new SqlJoin(
            POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            JoinType.valueOf(e.getJoinType().name()).symbol(SqlParserPos.ZERO),
            rightLateralAs,
            JoinConditionType.NONE.symbol(POS),
            (SqlNode) null);
    return this.result(join, leftResult, rightResult);
  }

  private void parseCorrelTable(RelNode relNode, SqlImplementor.Result x) {
    for (CorrelationId id : relNode.getVariablesSet()) {
      this.correlTableMap.put(id, x.qualifiedContext());
    }
  }

  /**
   * Uses only the object name (i.e. the last identifier) for the table names in a TableScan
   *
   * @param e
   * @return
   */
  @Override
  public SqlImplementor.Result visit(TableScan e) {
    var result = super.visit(e);
    if (result.node instanceof SqlIdentifier tableId) {
      if (tableId.names.size() > 1) {
        var simpleId =
            new SqlIdentifier(tableId.names.get(tableId.names.size() - 1), SqlParserPos.ZERO);
        return this.result(simpleId, ImmutableList.of(Clause.FROM), e, (Map) null);
      }
    }
    return result;
  }
}
