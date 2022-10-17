package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.*;

/**
 * Converts an expression into canonical form by fully-qualifying any identifiers. For common
 * columns in USING, it will be converted to COALESCE(A.col, B.col) AS col.
 */
public class SelectExpander2 extends Expander2 {

  final SqlSelect select;

  public SelectExpander2(SqlValidatorImpl validator, SelectScope scope,
      SqlSelect select) {
    super(validator, scope);
    this.select = select;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
//    final SqlNode node = expandCommonColumn(select, id, (SelectScope) getScope(), validator);
//    if (node != id) {
//      return node;
//    } else {
      return super.visit(id);
//    }
  }
}