package org.apache.calcite.sql;

public interface StatementVisitor<R, C> {

  R visit(SqrlCreateDefinition statement, C context);

  R visit(SqrlImportDefinition statement, C context);

  R visit(SqrlExportDefinition statement, C context);

  default R visit(SqrlExpressionQuery statement, C context) {
    return visit((SqrlAssignment) statement, context);
  }

  default R visit(SqrlSqlQuery statement, C context) {
    return visit((SqrlAssignment) statement, context);
  }

  default R visit(SqrlJoinQuery statement, C context) {
    return visit((SqrlAssignment) statement, context);
  }

  default R visit(SqrlFromQuery statement, C context) {
    return visit((SqrlAssignment) statement, context);
  }

  default R visit(SqrlDistinctQuery statement, C context) {
    return visit((SqrlAssignment) statement, context);
  }

  default R visit(SqrlAssignment statement, C context) {
    throw new RuntimeException("Could not walk statement: " + statement.getClass());
  }
}
