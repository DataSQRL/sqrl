package com.datasqrl.function;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.*;

import java.util.Optional;

@AllArgsConstructor
@Getter
public class SqrlFunctionParameter implements FunctionParameter {

  private final String name;
  private final Optional<SqlNode> defaultValue;
  private final SqlDataTypeSpec type;
  private final int ordinal;
  private final RelDataType relDataType;
  private final boolean isInternal;

  @Override
  public RelDataType getType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

  @Override
  public boolean isOptional() {
    return false;
  }
}
