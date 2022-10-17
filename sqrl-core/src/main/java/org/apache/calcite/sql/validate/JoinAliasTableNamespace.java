package org.apache.calcite.sql.validate;

import lombok.Getter;

public class JoinAliasTableNamespace extends TableNamespace {

  @Getter
  private final String alias;

  public JoinAliasTableNamespace(SqlValidatorImpl validator, SqlValidatorTable table, String alias) {
    super(validator, table);
    this.alias = alias;
  }
}
