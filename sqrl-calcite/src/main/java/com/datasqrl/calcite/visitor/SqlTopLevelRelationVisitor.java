package com.datasqrl.calcite.visitor;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

public interface SqlTopLevelRelationVisitor<R, C> {
  R visitQuerySpecification(SqlSelect node, C context);
  R visitOrderedUnion(SqlOrderBy node, C context);
  R visitSetOperation(SqlCall node, C context);
}
