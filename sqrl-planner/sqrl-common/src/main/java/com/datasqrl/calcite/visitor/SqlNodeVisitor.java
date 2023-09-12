package com.datasqrl.calcite.visitor;

import org.apache.calcite.sql.*;

public abstract class SqlNodeVisitor<R, C> implements
  SqlRelationVisitor<R, C>,
    StatementVisitor<R, C> {
  public static <R, C> R accept(SqlRelationVisitor<R, C> visitor, SqlNode node, C context) {
    if (node.getKind() == SqlKind.AS) {
      return visitor.visitAliasedRelation((SqlCall) node, context);
    } else if (node instanceof SqrlCompoundIdentifier) {
      return visitor.visitTable((SqrlCompoundIdentifier) node, context);
    } else if (node instanceof SqlIdentifier) {
      return visitor.visitTable((SqlIdentifier) node, context);
    } else if (node instanceof SqlJoin) {
      return visitor.visitJoin((SqlJoin) node, context);
    } else if (node instanceof SqlSelect) {
      return visitor.visitQuerySpecification((SqlSelect) node, context);
    } else if (node instanceof SqlCall
        && SqlKind.SET_QUERY.contains(node.getKind())) {
      return visitor.visitSetOperation((SqlCall) node, context);
    } else if (node instanceof SqlCall) {
      return visitor.visitTableFunction((SqlCall) node, context);
    }
    throw new RuntimeException("Unknown sql statement node:" + node);
  }

  @Override
  public R visitQuerySpecification(SqlSelect node, C context) {
    return visit((SqlNode) node, context);
  }

  @Override
  public R visitAliasedRelation(SqlCall node, C context) {
    return visit((SqlNode) node, context);
  }

  @Override
  public R visitTable(SqrlCompoundIdentifier node, C context) {
    return visit((SqlNode) node, context);
  }

  @Override
  public R visitJoin(SqlJoin node, C context) {
    return visit((SqlNode) node, context);
  }

  @Override
  public R visitSetOperation(SqlCall node, C context) {
    return visit((SqlNode) node, context);
  }

  @Override
  public R visit(SqrlImportDefinition statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlExportDefinition statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlStreamQuery statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlExpressionQuery statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlSqlQuery statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlJoinQuery statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlFromQuery statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlDistinctQuery statement, C context) {
    return visit((SqlNode) statement, context);
  }

  @Override
  public R visit(SqrlAssignTimestamp statement, C context) {
    return visit((SqlNode) statement, context);
  }

  public R visit(SqlNode node, C context) {
    throw new RuntimeException("Unknown sql node:" + node);
  }
}