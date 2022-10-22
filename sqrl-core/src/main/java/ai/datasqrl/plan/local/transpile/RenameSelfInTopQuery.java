package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.parse.tree.name.ReservedName;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Renames any user defined self tables
 */
@AllArgsConstructor
public class RenameSelfInTopQuery extends SqlShuttle {
  String selfVirtualTableName;

  public SqlNode accept(SqlNode query) {
    if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      select.setFrom(select.getFrom().accept(this));
    }
    return query;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (id.names.size() == 1 && id.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return new SqlIdentifier(selfVirtualTableName, SqlParserPos.ZERO);
    }

    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case AS:
        return SqlStdOperatorTable.AS.createCall(
            call.getParserPosition(),
            call.getOperandList().get(0).accept(this),
            call.getOperandList().get(1));

      case UNION:
      case INTERSECT:
      case EXCEPT:
      case SELECT:
        // (subquery, don't walk)
        return call;
    }
    return super.visit(call);
  }

}
