package org.apache.calcite.sql.validate;

import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

public class JoinDeclarationTableNamespace extends TableNamespace {

  private final SqlNode node;

  public JoinDeclarationTableNamespace(SqlValidatorImpl validator, SqlValidatorTable table,
      SqlNode node) {
    super(validator, table);
    this.node = node;
  }

  @Override
  public SqlNode getNode() {
    return node;
  }
}
