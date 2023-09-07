package com.datasqrl.function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

public class SqrlInternalFunctionParameter implements FunctionParameter {

  @Override
  public int getOrdinal() {
    return 0;
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return null;
  }

  @Override
  public boolean isOptional() {
    return false;
  }
}
