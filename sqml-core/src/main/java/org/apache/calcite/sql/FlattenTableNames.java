package org.apache.calcite.sql;

import java.util.List;

/**
 * Calcite handles arbitrarily nested table identifiers as nested schemas rather than
 *  nested tables. This prevents sqrl from being able to extract the necessary logical plan.
 *  This massages the identifier names as single identifier names rather than lists of identifiers.
 *
 * To be removed when we introduce our own parser.
 */
public class FlattenTableNames {

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
      for (SqlNode op : ((SqlBasicCall)node).getOperandList()) {
        if (op instanceof SqlIdentifier) {
          rewriteIdentifier((SqlIdentifier)op);
        } else {
          rewrite(op);
        }
      }
    } else if (node instanceof SqlIdentifier) {
      SqlIdentifier identifier = (SqlIdentifier) node;
      rewriteIdentifier(identifier);
    } else {
      System.out.println("Skipping: " + node);
    }
  }

  private static void rewriteIdentifier(SqlIdentifier identifier) {
    if (identifier.names.size() > 1) {
      identifier.setNames(List.of(String.join(".", identifier.names)),
          List.of(identifier.componentPositions.get(0)));
    }
  }
}
