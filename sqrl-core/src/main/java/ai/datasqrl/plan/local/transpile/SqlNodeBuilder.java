package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlNode;

public interface SqlNodeBuilder {
    SqlNode createTableNode(TableWithPK tableWithPK, String alias);
    SqlNode createSelfEquality(TableWithPK table, String lhsAlias, String rhsAlias);
    SqlNode createJoin(JoinType left, SqlNode lhs, SqlNode rhs, SqlNode condition);
  }
