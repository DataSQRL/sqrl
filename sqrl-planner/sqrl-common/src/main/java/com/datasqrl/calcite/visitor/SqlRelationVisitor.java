package com.datasqrl.calcite.visitor;

import org.apache.calcite.sql.*;

public interface SqlRelationVisitor<R, C> {
  R visitQuerySpecification(SqlSelect node, C context);
  R visitAliasedRelation(SqlCall node, C context);
  R visitTable(SqrlCompoundIdentifier node, C context);
  R visitJoin(SqlJoin node, C context);
  R visitSetOperation(SqlCall node, C context);
  default R visitTable(SqlIdentifier node, C context) {
    return null;
  }
  default R visitTableFunction(SqlCall node, C context) {
    return null;
  }
}