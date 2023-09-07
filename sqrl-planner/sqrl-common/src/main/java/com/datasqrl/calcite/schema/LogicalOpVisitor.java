package com.datasqrl.calcite.schema;

public interface LogicalOpVisitor<R, C> {
    R visit(LogicalSchemaModifyOps ops, C context);
    R visit(LogicalImportOp relNode, C context);
    R visit(LogicalAssignTimestampOp relNode, C context);
    R visit(LogicalExportOp relNode, C context);
    R visit(LogicalCreateReference relNode, C context);
    R visit(LogicalCreateAlias relNode, C context);
    R visit(LogicalCreateTableOp relNode, C context);
    R visit(LogicalCreateStreamOp relNode, C context);
    R visit(LogicalAddColumnOp relNode, C context);
  }