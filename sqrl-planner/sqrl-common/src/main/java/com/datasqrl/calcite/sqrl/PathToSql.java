package com.datasqrl.calcite.sqrl;

import com.datasqrl.calcite.NormalizeTablePath.PathItem;
import com.datasqrl.calcite.NormalizeTablePath.SelfTablePathItem;
import com.datasqrl.calcite.NormalizeTablePath.TableFunctionPathItem;
import com.datasqrl.canonicalizer.ReservedName;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@AllArgsConstructor
public class PathToSql {
  private final Deque<SqlNode> stack = new ArrayDeque<>();

  public SqlNode build(List<PathItem> pathItems) {
    for (int i = 0; i < pathItems.size(); i++) {
      PathItem item = pathItems.get(i);
      if (item instanceof SelfTablePathItem) {
        String tableName = ((SelfTablePathItem)item).getTable().getQualifiedName().get(0);
        SqlNode table = new SqlIdentifier(tableName, SqlParserPos.ZERO);
        SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(
            SqlParserPos.ZERO,
            table,
            new SqlIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), SqlParserPos.ZERO)
        );
        stack.push(aliasedCall);
      } else if (item instanceof TableFunctionPathItem) {
        TableFunctionPathItem tableFncItm = (TableFunctionPathItem) item;
        SqlCall call = tableFncItm.getOp().createCall(SqlParserPos.ZERO, tableFncItm.getArguments());
        call = SqlStdOperatorTable.COLLECTION_TABLE.createCall(SqlParserPos.ZERO, call);
        SqlCall aliasedCall = SqlStdOperatorTable.AS.createCall(
            SqlParserPos.ZERO,
            call,
            new SqlIdentifier(tableFncItm.getAlias(), SqlParserPos.ZERO)
        );
        stack.push(aliasedCall);
      }
      if (i > 0) {
        joinLateral();
      }
    }
    return stack.pop();
  }

  private void joinLateral() {
    SqlNode right = stack.pop();
    SqlNode left = stack.pop();

    SqlJoin join = new SqlJoin(
        SqlParserPos.ZERO,
        left,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
        SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, right),
        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
        null
    );

    stack.push(join);
  }

  @Value
  public static class PathToSqlResult {
    SqlNode sqlNode;
  }
}