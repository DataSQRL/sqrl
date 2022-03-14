package org.apache.calcite.sql;

import org.apache.calcite.sql.type.SqrlOperandTypeChecker;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;

public class SqrlBinaryOperator extends SqlMonotonicBinaryOperator {

  private final SqlBinaryOperator delegate;

  public SqrlBinaryOperator(SqlBinaryOperator delegate) {
   super(delegate.getName(),
       delegate.getKind(),
       0,
       false,
       delegate.getReturnTypeInference(),
       delegate.getOperandTypeInference(),
       new SqrlOperandTypeChecker(delegate.getOperandTypeChecker()));
    this.delegate = delegate;
  }

  @Override
  public int getLeftPrec() {
    return delegate.getLeftPrec();
  }

  @Override
  public int getRightPrec() {
    return delegate.getRightPrec();
  }
}
