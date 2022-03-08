package org.apache.calcite.sql;

import org.apache.calcite.sql.type.SqrlOperandTypeChecker;

public class SqrlFunctionOperator extends SqlFunction {

  private final SqlFunction delegate;

  public SqrlFunctionOperator(SqlFunction delegate) {
    super(delegate.getName(),
        delegate.getSqlIdentifier(),
        delegate.kind, delegate.getReturnTypeInference(),
        delegate.getOperandTypeInference(),
        new SqrlOperandTypeChecker(delegate.getOperandTypeChecker()),
        delegate.getFunctionType());
    this.delegate = delegate;
  }
}
