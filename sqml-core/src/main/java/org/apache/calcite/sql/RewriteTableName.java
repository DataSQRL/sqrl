package org.apache.calcite.sql;

import java.util.List;

public class RewriteTableName {

  public static void rewrite(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      rewrite(((SqlOrderBy)node).query);
    } else if (node instanceof SqlSelect) {
      SqlNode from = ((SqlSelect)node).getFrom();
      if (from instanceof SqlIdentifier) {
        SqlIdentifier identifier = (SqlIdentifier) from;
        rewriteIdentifier(identifier);
      } else {
        rewrite(from);
      }
    } else if (node instanceof SqlJoin) {
      rewrite(((SqlJoin)node).left);
      rewrite(((SqlJoin)node).right);
    } else if (node instanceof SqlBasicCall) {
      for (SqlNode op : ((SqlBasicCall)node).operands) {
        if (op instanceof SqlIdentifier) {
          rewriteIdentifier((SqlIdentifier)op);
        } else {
          rewrite(op);
        }
      }
    }
  }

  private static void rewriteIdentifier(SqlIdentifier identifier) {
    if (identifier.names.size() > 1) {
      identifier.setNames(List.of(String.join(".", identifier.names)),
          List.of(identifier.componentPositions.get(0)));
    }
  }
}
