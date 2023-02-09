/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.local.generate.SqlNodeFactory;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Qualifies all identifiers that could be potentially ambiguous during transpilation
 * <p>
 * Orders.entries2 := SELECT * FROM @.parent.entries; -> Orders.entries2 := SELECT x1.discount,
 * x1.unit_price, ... FROM @.parent.entries AS x1;
 */
public class QualifyIdentifiers extends SqlShuttle
    implements SqrlJoinTermVisitor<SqrlJoinTerm, Object> {

  private final Analysis analysis;

  public QualifyIdentifiers(Analysis analysis) {
    this.analysis = analysis;
  }

  //the only time we should be here is if we're a table identifier w/ no alias
  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.mayNeedAlias.get(id) != null) {
      SqlNodeFactory factory = new SqlNodeFactory(id.getParserPosition());
      return factory.callAs(id, analysis.mayNeedAlias.get(id));
    }

    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall node) {
    switch (node.getKind()) {
      case SELECT:
        return rewriteSelect((SqlSelect) node);
      case JOIN_DECLARATION:
        return rewriteJoinDeclaration((SqrlJoinDeclarationSpec) node);
    }

    return super.visit(node);
  }

  private SqlNode rewriteJoinDeclaration(SqrlJoinDeclarationSpec node) {
    SqrlJoinTerm relation = node.getRelation().accept(this, null);
    Optional<SqlNodeList> orders = node.getOrderList().map(o -> (SqlNodeList) expr(o));

    return new SqrlJoinDeclarationSpec(
        node.getParserPosition(),
        relation,
        orders,
        node.fetch,
        node.inverse,
        node.leftJoins
    );
  }

  @Override
  public SqrlJoinTerm visitJoinPath(SqrlJoinPath sqrlJoinPath, Object context) {
    return new SqrlJoinPath(sqrlJoinPath.getParserPosition(),
        mapRelations(sqrlJoinPath),
        mapConditions(sqrlJoinPath));
  }

  private List<SqlNode> mapRelations(SqrlJoinPath sqrlJoinPath) {
    return sqrlJoinPath.relations.stream()
        .map(i -> i.accept(this))
        .collect(Collectors.toList());
  }

  private List<SqlNode> mapConditions(SqrlJoinPath sqrlJoinPath) {
    return sqrlJoinPath.conditions.stream()
        .map(i -> i != null ? expr(i) : null)
        .collect(Collectors.toList());
  }

  @Override
  public SqrlJoinTerm visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation,
      Object context) {
    return null;
  }

  private SqlNode rewriteSelect(SqlSelect select) {
    select.setFrom(select.getFrom().accept(this));

    List<SqlNode> expandedSelect = analysis.expandedSelect.get(select);
    SqlNodeList nodeList = new SqlNodeList(expandedSelect,
        select.getSelectList().getParserPosition());
    select.setSelectList((SqlNodeList) expr(nodeList));

    if (select.getWhere() != null) {
      select.setWhere(expr(select.getWhere()));
    }

    if (select.getGroup() != null) {
      select.setGroupBy(replaceGroupBy(select.getGroup()));
    }
    if (select.getHaving() != null) {
      select.setHaving(expr(select.getHaving()));
    }

    if (select.getOrderList() != null) {
      select.setOrderBy((SqlNodeList) expr(select.getOrderList()));
    }

    return select;
  }

  private SqlNodeList replaceGroupBy(SqlNodeList group) {
    List<SqlNode> full = analysis.groupByExpressions.get(group)
        .stream()
        .map(this::expr)
        .collect(Collectors.toList());

    return new SqlNodeList(full, group.getParserPosition());
  }

  public SqlNode expr(SqlNode node) {
    return node.accept(new QualifyExpression());
  }

  public class QualifyExpression extends SqlShuttle {

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {

        case SELECT:
        case UNION:
        case INTERSECT:
        case EXCEPT:
          QualifyIdentifiers qualifyIdentifiers = new QualifyIdentifiers(analysis);

          return call.accept(qualifyIdentifiers);

      }
      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (analysis.getExpressions().containsKey(id)) {
        return analysis.getExpressions().get(id).getAliasedIdentifier(id);
      }

      return super.visit(id);
    }
  }
}
