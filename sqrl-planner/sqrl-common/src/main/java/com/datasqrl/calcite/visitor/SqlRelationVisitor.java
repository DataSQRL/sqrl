package com.datasqrl.calcite.visitor;

import org.apache.calcite.sql.*;

public interface SqlRelationVisitor<R, C> {
  R visitQuerySpecification(SqlSelect node, C context);
  R visitAliasedRelation(SqlCall node, C context);
  R visitTable(SqlIdentifier node, C context);
  R visitJoin(SqlJoin node, C context);
  R visitSetOperation(SqlCall node, C context);
  R visitCollectTableFunction(SqlCall node, C context);
  R visitLateralFunction(SqlCall node, C context);
  R visitUnnestFunction(SqlCall node, C context);
  R visitUserDefinedTableFunction(SqlCall node, C context);
  R visitCall(SqlCall node, C context);
}