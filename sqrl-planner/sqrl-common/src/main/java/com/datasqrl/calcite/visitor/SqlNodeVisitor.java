package com.datasqrl.calcite.visitor;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLateralOperator;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqrlAssignTimestamp;
import org.apache.calcite.sql.SqrlAssignment;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlStreamQuery;
import org.apache.calcite.sql.StatementVisitor;
import org.apache.calcite.sql.fun.SqlCollectionTableOperator;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;

public abstract class SqlNodeVisitor<R, C> implements
    SqlRelationVisitor<R, C>,
    StatementVisitor<R, C> {

  public static <R, C> R accept(StatementVisitor<R, C> visitor, SqlNode node, C context) {
    if (node instanceof SqrlImportDefinition) {
      return visitor.visit((SqrlImportDefinition) node, context);
    } else if (node instanceof SqrlExportDefinition) {
      return visitor.visit((SqrlExportDefinition) node, context);
    } else if (node instanceof SqrlStreamQuery) {
      return visitor.visit((SqrlStreamQuery) node, context);
    } else if (node instanceof SqrlExpressionQuery) {
      return visitor.visit((SqrlExpressionQuery) node, context);
    } else if (node instanceof SqrlSqlQuery) {
      return visitor.visit((SqrlSqlQuery) node, context);
    } else if (node instanceof SqrlJoinQuery) {
      return visitor.visit((SqrlJoinQuery) node, context);
    } else if (node instanceof SqrlFromQuery) {
      return visitor.visit((SqrlFromQuery) node, context);
    } else if (node instanceof SqrlDistinctQuery) {
      return visitor.visit((SqrlDistinctQuery) node, context);
    } else if (node instanceof SqrlAssignTimestamp) {
      return visitor.visit((SqrlAssignTimestamp) node, context);
    } else if (node instanceof SqrlAssignment) {
      return visitor.visit((SqrlAssignment) node, context);
    }
    throw new RuntimeException("Unknown sql statement node:" + node);
  }

  public static <R, C> R accept(SqlTopLevelRelationVisitor<R, C> visitor, SqlNode node, C context) {
    Preconditions.checkNotNull(node, "Could not rewrite query.");
    if (node instanceof SqlSelect) {
      return visitor.visitQuerySpecification((SqlSelect) node, context);
    } else if (node instanceof SqlCall
        && SqlKind.SET_QUERY.contains(node.getKind())) {
      return visitor.visitSetOperation((SqlCall) node, context);
    } else if (node instanceof SqlCall && node.getKind() == SqlKind.ORDER_BY &&
        ((SqlCall) node).getOperandList().get(0).getKind() == SqlKind.UNION) {
      return visitor.visitOrderedUnion((SqlOrderBy) node, context);
    }
    throw new RuntimeException("Unknown sql statement node:" + node);
  }

  public static <R, C> R accept(SqlRelationVisitor<R, C> visitor, SqlNode node, C context) {
    Preconditions.checkNotNull(node, "Could not rewrite query.");
    if (node.getKind() == SqlKind.AS) {
      return visitor.visitAliasedRelation((SqlCall) node, context);
    } else if (node instanceof SqlIdentifier) {
      return visitor.visitTable((SqlIdentifier) node, context);
    } else if (node instanceof SqlJoin) {
      return visitor.visitJoin((SqlJoin) node, context);
    } else if (node instanceof SqlSelect) {
      return visitor.visitQuerySpecification((SqlSelect) node, context);
    } else if (node instanceof SqlCall
        && SqlKind.SET_QUERY.contains(node.getKind())) {
      return visitor.visitSetOperation((SqlCall) node, context);
    } else if (node instanceof SqlCall && ((SqlCall) node).getOperator() instanceof SqlCollectionTableOperator) {
      return visitor.visitCollectTableFunction((SqlCall) node, context);
    } else if (node instanceof SqlCall && ((SqlCall) node).getOperator() instanceof SqlLateralOperator) {
      return visitor.visitLateralFunction((SqlCall) node, context);
    } else if (node instanceof SqlCall && ((SqlCall) node).getOperator() instanceof SqlUnnestOperator) {
      return visitor.visitUnnestFunction((SqlCall) node, context);
    } else if (node instanceof SqlCall &&
        ((SqlCall) node).getOperator() instanceof SqlUserDefinedTableFunction) {
      return visitor.visitUserDefinedTableFunction((SqlCall) node, context);
    } else if (node instanceof SqlCall &&
        ((SqlCall) node).getOperator() instanceof SqlUnresolvedFunction) {
      return visitor.visitUserDefinedTableFunction((SqlCall) node, context);
    } else if (node instanceof SqlCall && node.getKind() == SqlKind.ORDER_BY &&
        ((SqlCall) node).getOperandList().get(0).getKind() == SqlKind.UNION) {
      return visitor.visitOrderedUnion((SqlOrderBy) node, context);
    } else if (node instanceof SqlCall) {
      return visitor.visitCall((SqlCall) node, context);
    }
    throw new RuntimeException("Unknown sql statement node:" + node);
  }
}