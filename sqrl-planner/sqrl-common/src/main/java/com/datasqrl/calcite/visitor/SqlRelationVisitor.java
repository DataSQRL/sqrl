package com.datasqrl.calcite.visitor;

import org.apache.calcite.sql.*;

public interface SqlRelationVisitor<R, C> {
  R visitQuerySpecification(SqlSelect node, C context);
  R visitAliasedRelation(SqlCall node, C context);
  R visitTable(SqlIdentifier node, C context);
  R visitJoin(SqlJoin node, C context);
  R visitSetOperation(SqlCall node, C context);
  R visitTableFunction(SqlCall node, C context);
  R visitCall(SqlCall node, C context);
}