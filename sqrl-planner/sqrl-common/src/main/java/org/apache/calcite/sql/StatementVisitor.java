package org.apache.calcite.sql;

public interface StatementVisitor<R, C> {
    R visit(SqrlImportDefinition statement, C context);
    R visit(SqrlExportDefinition statement, C context);
    R visit(SqrlStreamQuery statement, C context);
    R visit(SqrlExpressionQuery statement, C context);
    R visit(SqrlSqlQuery statement, C context);
    R visit(SqrlJoinQuery statement, C context);
    R visit(SqrlFromQuery statement, C context);
    R visit(SqrlDistinctQuery statement, C context);
    R visit(SqrlAssignTimestamp statement, C context);
}
