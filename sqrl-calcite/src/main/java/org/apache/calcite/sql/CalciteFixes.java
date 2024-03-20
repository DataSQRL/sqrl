package org.apache.calcite.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;

public class CalciteFixes {

  public static List<SqlParserPos> getComponentPositions(SqlIdentifier identifier) {
    return identifier.componentPositions == null ? List.of() :
        new ArrayList<>(identifier.componentPositions);
  }

  public static void appendSelectLists(SqlNode node) {
    //calcite quirk
    node.accept(new SqlShuttle() {
      @Override
      public SqlNode visit(SqlCall call) {
        if (call.getKind() == SqlKind.SELECT) {
          SqlSelect select1 = ((SqlSelect) call);

          if (select1.getSelectList() == null) {
            select1.setSelectList(new SqlNodeList(List.of(SqlIdentifier.STAR), SqlParserPos.ZERO));
          }
        }

        return super.visit(call);
      }
    });
  }

  //odd behavior by calcite parser, im doing something wrong?
  public static SqlNode pushDownOrder(SqlNode sqlNode) {
    //recursive?
    if (sqlNode instanceof SqlOrderBy && ((SqlOrderBy) sqlNode).query instanceof SqlSelect ) {
      SqlOrderBy order = (SqlOrderBy) sqlNode;
      SqlSelect select = ((SqlSelect) order.query);
      select.setOrderBy(order.orderList);
      select.setFetch(order.fetch);
      select.setOffset(order.offset);
      sqlNode = select;
    }
    return sqlNode;
  }
}
