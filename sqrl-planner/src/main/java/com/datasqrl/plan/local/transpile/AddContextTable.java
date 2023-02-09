/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.name.ReservedName;
import com.datasqrl.plan.local.generate.SqlNodeFactory;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Orders.x := SELECT * FROM Product; -> Orders.x := SELECT * FROM orders$1 AS _ JOIN Product;
 * <p>
 * Does not add self table if one is already present and is properly aliased. Also adds self to
 * expressions.
 */
@AllArgsConstructor
public class AddContextTable
    extends SqlShuttle
    implements SqrlJoinTermVisitor<SqlNode, Object> {

  private final boolean hasContext;

  @Override
  public SqlNode visit(SqlCall node) {
    if (!hasContext) {
      return node;
    }

    ValidateNoReservedAliases noReservedAliases = new ValidateNoReservedAliases();
    node.accept(noReservedAliases);

    SqlNodeFactory factory = new SqlNodeFactory(node.getParserPosition());

    switch (node.getKind()) {
      case JOIN_DECLARATION:
        return visitJoinDeclaration(node);
      case ORDER_BY:
        return visitOrderBy(node);
      case UNION:
        return visitUnion(node);
      case JOIN:
        return visitJoin(node);
      case AS:
        return visitAs(node,factory);
      case SELECT:
        return visitSelect(node,factory);
    }
    return super.visit(node);
  }

  private SqlNode visitJoinDeclaration(SqlCall node) {
    SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) node;
    SqrlJoinTerm term = spec.getRelation();
    SqrlJoinTerm newTerm = (SqrlJoinTerm) term.accept(this, null);

    return new SqrlJoinDeclarationSpec(spec.getParserPosition(),
        newTerm,
        spec.orderList,
        spec.fetch,
        spec.inverse,
        spec.getLeftJoins());
  }

  private SqlNode visitOrderBy(SqlCall node) {
    SqlOrderBy order = (SqlOrderBy) node;

    return new SqlOrderBy(order.getParserPosition(),
        order.query.accept(this), order.orderList, order.offset, order.fetch);
  }

  private SqlNode visitUnion(SqlCall node) {
    SqlBasicCall call = (SqlBasicCall) node;
    SqlNode[] operands = call.getOperandList().stream()
        .map(o -> o.accept(this))
        .toArray(SqlNode[]::new);

    return new SqlBasicCall(call.getOperator(), operands, call.getParserPosition());
  }

  private SqlNode visitJoin(SqlCall node) {
    SqlJoin join = (SqlJoin) node;
    SqlNode node2 = join.getLeft();
    SqlNode newLeft = node2.accept(this);
    join.setLeft(newLeft);

    return join;
  }

  private SqlNode visitAs(SqlCall node, SqlNodeFactory factory) {
    return addSelfLeftDeep(node, factory);
  }

  private SqlNode visitSelect(SqlCall node, SqlNodeFactory factory) {
    SqlSelect select = (SqlSelect) node;
    if (select.getFrom() == null) {
      select.setFrom(factory.createSelf());
    } else {
      select.setFrom(select.getFrom().accept(this));
    }

    return select;
  }

  @Override
  public SqlNode visitJoinPath(SqrlJoinPath node, Object context) {
    //Check to see if first relation is self, if so, return
    if (isSelfTable(node.relations.get(0))) {
      return node;
    }

    List<SqlNode> relations = new ArrayList<>();
    relations.add(new SqlNodeFactory(node.getParserPosition()).createSelf());
    relations.addAll(node.getRelations());
    List<SqlNode> conditions = new ArrayList<>();
    conditions.add(null);
    conditions.addAll(node.getConditions());

    return new SqrlJoinPath(
        node.getParserPosition(),
        relations,
        conditions);
  }

  private boolean isSelfTable(SqlNode node) {
    return SelfTableChecker.hasSelfTable(node);
  }

  @Override
  public SqlNode visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation, Object context) {
    //todo
    return null;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    SqlNodeFactory factory = new SqlNodeFactory(id.getParserPosition());
    return LeftDeepAdder.addSelfLeftDeep(id, factory);
  }

  private SqlNode addSelfLeftDeep(SqlNode node, SqlNodeFactory factory) {
    return factory.createJoin(factory.createSelf(), JoinType.DEFAULT, node);
  }

  class ValidateNoReservedAliases extends SqlBasicVisitor<SqlNode> {

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case AS:
          validateNotReserved((SqlIdentifier) call.getOperandList().get(1));
      }
      return super.visit(call);
    }

    private void validateNotReserved(SqlIdentifier identifier) {
      Preconditions.checkState(identifier.names.size() == 1 &&
              !identifier.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical()),
          "The SELF keyword is reserved, use a different alias"
      );
    }
  }
  public static class SelfTableChecker {

    public static boolean hasSelfTable(SqlNode node) {
      switch (node.getKind()) {
        case AS:
          SqlCall call = (SqlCall) node;
          SqlNode table = call.getOperandList().get(0);
          return hasSelfTable(table);
        case IDENTIFIER:
          SqlIdentifier identifier = (SqlIdentifier) node;
          if (identifier.names.size() == 1 && identifier.names.get(0)
              .equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
            return true;
          }
      }
      return false;
    }
  }

  /**
   * This class handles the logic to add self left deep
   */
  public static class LeftDeepAdder {

    public static SqlNode addSelfLeftDeep(SqlIdentifier node, SqlNodeFactory factory) {
      //Already exists
     if (node.names.size() == 1 && node.names.get(0)
        .equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
       return factory.createSelf();
     }

     return factory.createJoin(factory.createSelf(),
         JoinType.DEFAULT, node);
    }
  }

}
