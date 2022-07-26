package org.apache.calcite.sql;

import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;

public interface SqlNodeBuilder {
    SqlNode createTableNode(TableWithPK tableWithPK, String alias);
    SqlNode createSelfEquality(TableWithPK table, String lhsAlias, String rhsAlias);
    SqlNode createJoin(JoinType left, SqlNode lhs, SqlNode rhs, SqlNode condition);
  }
