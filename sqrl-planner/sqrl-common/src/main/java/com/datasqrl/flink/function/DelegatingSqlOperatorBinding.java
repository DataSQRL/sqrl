package com.datasqrl.flink.function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources.ExInst;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

public class DelegatingSqlOperatorBinding extends SqlOperatorBinding {
    SqlOperatorBinding delegate;

    protected DelegatingSqlOperatorBinding(FlinkTypeFactory typeFactory, SqlOperatorBinding delegate) {
      super(typeFactory, delegate.getOperator());
      this.delegate = delegate;
    }

    @Override
    public int getOperandCount() {
      return delegate.getOperandCount();
    }

    @Override
    public RelDataType getOperandType(int i) {
      return delegate.getOperandType(i);
    }

    @Override
    public CalciteException newError(ExInst<SqlValidatorException> exInst) {
      return delegate.newError(exInst);
    }
  }