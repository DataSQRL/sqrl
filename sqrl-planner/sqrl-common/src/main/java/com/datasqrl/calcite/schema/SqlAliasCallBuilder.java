package com.datasqrl.calcite.schema;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlAliasCallBuilder {

  private final SqlCall node;

  public SqlAliasCallBuilder(SqlCall node) {
    Preconditions.checkState(node.getKind() == SqlKind.AS);
    this.node = node.getOperator().createCall(SqlParserPos.ZERO,
        node.getOperandList().get(0),
        node.getOperandList().get(1));
  }

  public String getAlias() {
    String alias = ((SqlIdentifier)node.getOperandList().get(1)).getSimple();
    return alias;
  }

  public SqlAliasCallBuilder setTable(SqlNode sqlNode) {
    this.node.setOperand(0, sqlNode);
    return this;
  }

  public SqlNode build() {
    return node;
  }
}
