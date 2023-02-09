/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.SqlNodeUtil;
import com.datasqrl.plan.local.generate.SqlNodeFactory;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.AbsoluteResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.RelativeResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTableField;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.SingleTable;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.UnboundJoin;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class ReplaceWithVirtualTable extends SqlShuttle
    implements SqrlJoinTermVisitor<SqrlJoinTerm, Object> {

  private final Analysis analysis;

  Stack<SqlNode> pullup = new Stack<>();

  AtomicInteger aliasCnt = new AtomicInteger(0);

  public ReplaceWithVirtualTable(Analysis analysis) {

    this.analysis = analysis;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case UNBOUND_JOIN:
        return handleUnboundJoin(call);
      case JOIN_DECLARATION:
        return handleJoinDeclaration(call);
      case AS:
        return handleAs(call);
      case SELECT:
        return handleSelect(call);
      case JOIN:
        return handleJoin(call);
    }

    return super.visit(call);
  }

  private SqlNode handleUnboundJoin(SqlCall call) {
    UnboundJoin u = (UnboundJoin) call;
    return new UnboundJoin(u.getParserPosition(),
        u.getRelation().accept(this),
        u.getCondition()
    );
  }

  private SqlNode handleJoinDeclaration(SqlCall call) {
    SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) call;
    SqrlJoinTerm relation = spec.getRelation().accept(this, null);
    Optional<SqlNodeList> orderList = spec.getOrderList()
        .map(o -> (SqlNodeList) o.accept(this));
    Optional<SqlNodeList> leftJoin = spec.getLeftJoins().map(l -> (SqlNodeList) l.accept(this));

    return new SqrlJoinDeclarationSpec(spec.getParserPosition(),
        relation,
        orderList,
        spec.getFetch(),
        spec.getInverse(),
        leftJoin);
  }

  private SqlNode handleAs(SqlCall call) {
    SqlNode tbl = call.getOperandList().get(0);
    //aliased in inlined
    if (tbl instanceof SqlIdentifier) {
      ResolvedTable resolved = analysis.getTableIdentifiers().get(tbl);
      if (resolved instanceof RelativeResolvedTable &&
          ((RelativeResolvedTable) resolved).getFields().get(0).getJoin().isPresent()) {
        return tbl.accept(this);
      }
    }

    return super.visit(call);
  }

  private SqlNode handleSelect(SqlCall call) {
    SqlSelect select = (SqlSelect) super.visit(call);
    while (!pullup.isEmpty()) {
      SqlNode condition = pullup.pop();

      appendToSelect(select, condition);
    }
    return select;
  }

  private SqlNode handleJoin(SqlCall call) {
    SqlJoin join = (SqlJoin) super.visit(call);
    if (!pullup.isEmpty()) {
      SqlNode condition = pullup.pop();
      addJoinCondition(join, condition);
    }

    return join;
  }

  public static void addJoinCondition(SqlJoin join, SqlNode condition) {
    join.setOperand(5, concat(join.getCondition(), condition));
    join.setOperand(4, JoinConditionType.ON.symbol(SqlParserPos.ZERO));
  }


  private void appendToSelect(SqlSelect select, SqlNode condition) {
    select.setWhere(concat(condition,SqlNodeUtil.and(select.getWhere(), condition)));
  }

  private static SqlNode concat(SqlNode condition1, SqlNode condition2) {
    if (condition1 == null) {
      return condition2;
    } else {
      if (condition2 == null) {
        return condition1;
      } else {
        return SqlNodeUtil.and(condition1, condition2);
      }
    }
  }
  /**
   * Visits an {@link SqlIdentifier} and returns the corresponding SqlNode.
   *
   * @param id the {@link SqlIdentifier} to visit
   * @return the corresponding SqlNode
   */
  @Override
  public SqlNode visit(SqlIdentifier id) {
    // Check if identifier is an expression
    if (analysis.getExpressions().get(id) != null) {
      return id.accept(new ShadowColumns());
    }

    // Check if identifier is a table identifier
    if (analysis.getTableIdentifiers().get(id) == null) {
      return super.visit(id);
    }

    // Resolve the table identifier
    ResolvedTable resolved = analysis.getTableIdentifiers().get(id);
    return resolveTableIdentifier(resolved, id);
  }

  /**
   * Resolves the table identifier.
   *
   * @param resolved the {@link ResolvedTable}
   * @param id the {@link SqlIdentifier}
   * @return the corresponding SqlNode
   */
  private SqlNode resolveTableIdentifier(ResolvedTable resolved, SqlIdentifier id) {
    // Identifier is a single table
    SqlNodeFactory factory = new SqlNodeFactory(id.getParserPosition());
    if (resolved instanceof SingleTable) {
      SingleTable singleTable = (SingleTable) resolved;
      return new SqlIdentifier(singleTable.getToTable().getVt().getNameId(),
          id.getParserPosition());
    }

    // Identifier is an absolute resolved table
    if (resolved instanceof AbsoluteResolvedTable) {
      throw new RuntimeException("unexpected type");
    }

    // Identifier is a relative resolved table
    if (resolved instanceof RelativeResolvedTable) {
      RelativeResolvedTable resolveRel = (RelativeResolvedTable) resolved;
      Preconditions.checkState(resolveRel.getFields().size() == 1);
      Relationship relationship = resolveRel.getFields().get(0);

      // Check if join declaration
      if (relationship.getJoin().isPresent()) {
        String alias = analysis.tableAlias.get(id);
        ExpandJoinDeclaration expander = new ExpandJoinDeclaration(resolveRel.getAlias(), alias,
            aliasCnt);
        UnboundJoin pair = expander.expand(relationship.getJoin().get());
        pair.getCondition().map(c -> pullup.push(c));
        return pair.getRelation();
      }

      // Handle normal relationship
      String alias = analysis.tableAlias.get(id);
      SqlNode condition = createCondition(alias, resolveRel.getAlias(),
          relationship.getFromTable(), relationship.getToTable(), factory);
      pullup.push(condition);
      return factory.toIdentifier(relationship.getToTable().getVt().getNameId());
    }

    return super.visit(id);
  }

  public static SqlNode createCondition(String firstAlias, String alias, SQRLTable from,
      SQRLTable to, SqlNodeFactory factory) {
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < from.getVt().getPrimaryKeyNames().size()
        && i < to.getVt().getPrimaryKeyNames().size(); i++) {
      SqlNode lhs = factory.toIdentifier(alias, getPrimaryKeyName(from.getVt(), i));
      SqlNode rhs = factory.toIdentifier(firstAlias, getPrimaryKeyName(to.getVt(), i));
      SqlCall call = factory.callEq(lhs, rhs);

      conditions.add(call);
    }
    return SqlNodeUtil.and(conditions);
  }

  private static String getPrimaryKeyName(VirtualRelationalTable vt, int i) {
    return vt.getPrimaryKeyNames().get(i);
  }

  @Override
  public SqrlJoinTerm visitJoinPath(SqrlJoinPath node, Object context) {
    List<SqlNode> relations = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < node.getRelations().size(); i++) {
      //Note: this function may add to the pullup object. After we process, empty the pullup conditions
      relations.add(node.getRelations().get(i).accept(this));
      if (node.getConditions().get(i) != null) {
        conditions.add(node.getConditions().get(i).accept(this));
      } else {
        conditions.add(null);
      }
      if (!pullup.isEmpty()) {
        SqlNode condition = pullup.pop();
        conditions.set(i, SqlNodeUtil.and(conditions.get(i), condition));
      }
    }

    return new SqrlJoinPath(node.getParserPosition(),
        relations,
        conditions);
  }

  @Override
  public SqrlJoinTerm visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation,
      Object context) {
    return null;
  }

  private class ShadowColumns extends SqlShuttle {

    @Override
    public SqlNode visit(SqlIdentifier id) {
      ResolvedTableField field = analysis.getExpressions().get(id);
      if (field != null) {
        return field.getShadowedIdentifier(id);
      }
      return super.visit(id);
    }
  }
}
