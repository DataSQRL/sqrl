package org.apache.calcite.sql;

public interface StatementVisitor<R, C> {
    R visit(ImportDefinition statement, C context);
    R visit(ExportDefinition statement, C context);
    R visit(StreamAssignment statement, C context);
    R visit(ExpressionAssignment statement, C context);
    R visit(QueryAssignment statement, C context);
    R visit(JoinAssignment statement, C context);
    R visit(DistinctAssignment statement, C context);
}
