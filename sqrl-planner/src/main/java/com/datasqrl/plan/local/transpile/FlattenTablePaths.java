/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.local.generate.SqlNodeFactory;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.AbsoluteResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.RelativeResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.SingleTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.VirtualResolvedTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.UnboundJoin;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Orders.entries.customer = SELECT __a1.customerid FROM @.entries.parent p LEFT JOIN e.parent AS
 * __a1; -> Orders.entries.customer = SELECT __a1.customerid FROM (@.entries AS g1 JOIN g1.parent AS
 * p) LEFT JOIN e.parent AS __a1;
 */
public class FlattenTablePaths extends SqlShuttle
    implements SqrlJoinTermVisitor<SqrlJoinTerm, Object> {

  private final Analysis analysis;

  public FlattenTablePaths(Analysis analysis) {
    this.analysis = analysis;
  }

  public SqlNode accept(SqlNode node) {
    SqlNode result = node.accept(this);
    return result;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case JOIN_DECLARATION:
        SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) call;
        SqrlJoinTerm relation = spec.getRelation().accept(this, null);
        Optional<SqlNodeList> leftJoins = spec.getLeftJoins()
            .map(this::convertUnboundJoins);

        return new SqrlJoinDeclarationSpec(spec.getParserPosition(),
            relation,
            spec.getOrderList(),
            spec.getFetch(),
            spec.getInverse(),
            leftJoins);
      case JOIN:
        return super.visit(call);
      case AS:
        //When we rewrite paths, we turn a left tree into a bushy tree and we'll need to add the
        // alias to the last table. So If we do that, we need to remove this alias or calcite
        // will end up wrapping it in a subquery. This is for aesthetics so remove it if it causes
        // problems.
        if (analysis.getTableIdentifiers().get(call.getOperandList().get(0)) != null) {
          return call.getOperandList().get(0).accept(this);
        }
    }

    return super.visit(call);
  }

  private SqlNodeList convertUnboundJoins(SqlNodeList l) {
    return new SqlNodeList(l.getList().stream()
        .map(i -> {
          UnboundJoin j = (UnboundJoin) i;
          return new UnboundJoin(i.getParserPosition(), j.getRelation().accept(this),
              j.getCondition());
        })
        .collect(Collectors.toList()), SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.getTableIdentifiers().get(id) != null) {
      return expandTable(id);
    }
    return super.visit(id);
  }

  @Override
  public SqrlJoinTerm visitJoinPath(SqrlJoinPath sqrlJoinPath, Object context) {
    List<SqlNode> relation = sqrlJoinPath.getRelations().stream()
        .map(rel -> rel.accept(this))
        .collect(Collectors.toList());

    return new SqrlJoinPath(sqrlJoinPath.getParserPosition(),
        relation,
        sqrlJoinPath.getConditions());
  }

  @Override
  public SqrlJoinTerm visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation,
      Object context) {
    throw new RuntimeException("SET operations TBD");
  }

  @Value
  class ExpandedTable {

    SqlNode table;
    Optional<SqlNode> pullupCondition;
  }

  int idx = 0;

  private SqlNode expandTable(SqlIdentifier id) {
    String finalAlias = analysis.tableAlias.get(id);

    SqlNodeFactory factory = new SqlNodeFactory(id.getParserPosition());
    // Get the ResolvedTable for the SqlIdentifier
    ResolvedTable resolve = analysis.tableIdentifiers.get(id);

    // Depending on the ResolvedTable type, build the SqlIdentifier and the suffix
    List<String> suffix = getSuffix(id, resolve);
    SqlIdentifier first = getFirstIdentifier(id, resolve, factory);

    // Create the first SqlNode
    String firstAlias = "_g" + (++idx);
    SqlNode n = factory.callAs(first, suffix.size() > 0 ? firstAlias : finalAlias);

    // If there are suffixes, build the SqlNode by joining
    if (suffix.size() >= 1) {
      n = buildSuffixSqlNode(suffix, finalAlias, firstAlias, n, factory);
    }

    return n;
  }

  private SqlIdentifier getFirstIdentifier(SqlIdentifier id, ResolvedTable resolve,
      SqlNodeFactory factory) {
    if (resolve instanceof SingleTable || resolve instanceof VirtualResolvedTable
        || resolve instanceof AbsoluteResolvedTable) {
      return factory.toIdentifier(id.names.get(0));
    } else if (resolve instanceof RelativeResolvedTable) {
      return factory.toIdentifier(id.names.get(0), id.names.get(1));
    } else {
      throw new RuntimeException("");
    }
  }

  private List<String> getSuffix(SqlIdentifier id, ResolvedTable resolve) {
    if (resolve instanceof SingleTable || resolve instanceof VirtualResolvedTable) {
      return List.of();
    } else if (resolve instanceof AbsoluteResolvedTable) {
      return id.names.subList(1, id.names.size());
    } else if (resolve instanceof RelativeResolvedTable) {
      if (id.names.size() > 2) {
        return id.names.subList(2, id.names.size());
      } else {
        return List.of();
      }
    } else {
      throw new RuntimeException("");
    }
  }

  /**
   * Builds the SqlNode by joining the suffixes.
   */
  private SqlNode buildSuffixSqlNode(List<String> suffix, String finalAlias, String firstAlias,
      SqlNode n,
      SqlNodeFactory factory) {
    String currentAlias = firstAlias;

    // Iterate through the suffixes and join them
    for (int i = 0; i < suffix.size(); i++) {
      String nextAlias = i == suffix.size() - 1 ? finalAlias : "_g" + (++idx);
      SqlNode rhs = factory.callAs(factory.toIdentifier(currentAlias, suffix.get(i)), nextAlias);
      n = factory.createJoin(n, JoinType.DEFAULT, rhs);
      currentAlias = nextAlias;
    }
    return n;
  }
}
