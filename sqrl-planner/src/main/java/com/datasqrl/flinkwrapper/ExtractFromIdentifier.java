package com.datasqrl.flinkwrapper;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlVisitor;

public class ExtractFromIdentifier implements SqlVisitor<Boolean> {

  @Override
  public Boolean visit(SqlCall sqlCall) {
    return sqlCall.getOperandList().stream().anyMatch(sqlNode -> sqlNode.accept(this));
  }

  @Override
  public Boolean visit(SqlNodeList nodeList) {
    return nodeList.stream().anyMatch(sqlNode -> sqlNode.accept(this));
  }

  @Override
  public Boolean visit(SqlIdentifier identifier) {
    return true;
  }

  @Override
  public Boolean visit(SqlLiteral literal) {
    return false;
  }

  @Override
  public Boolean visit(SqlDataTypeSpec type) {
    return false;
  }

  @Override
  public Boolean visit(SqlDynamicParam param) {
    return false;
  }

  @Override
  public Boolean visit(SqlIntervalQualifier intervalQualifier) {
    return false;
  }
}