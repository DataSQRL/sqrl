package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import java.util.List;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlNodeBuilder {

  public static SqlNode createTableNode(TableWithPK tableWithPK, String alias) {
    return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
        new SqlIdentifier(tableWithPK.getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
        new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  public static SqlNode createSelfEquality(TableWithPK table, String lhsAlias, String rhsAlias) {
    for (String name : table.getPrimaryKeys()) {
      return new SqlBasicCall(SqrlOperatorTable.EQUALS,
          new SqlNode[]{new SqlIdentifier(List.of(lhsAlias, name), SqlParserPos.ZERO),
              new SqlIdentifier(List.of(rhsAlias, name), SqlParserPos.ZERO)}, SqlParserPos.ZERO);
    }

    return null;
  }

  public static SqlNode createJoin(JoinType type, SqlNode lhs, SqlNode rhs, SqlNode condition) {
    SqlLiteral joinType = SqlLiteral.createSymbol(type, SqlParserPos.ZERO);

    return new SqlJoin(SqlParserPos.ZERO, lhs, SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        joinType, rhs, SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO), condition);
  }

  public static SqlNode as(SqlNode sqlNode, String alias) {
    return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{SqlUtil.stripAs(sqlNode),
        new SqlIdentifier(alias, sqlNode.getParserPosition())}, SqlParserPos.ZERO);
  }
}