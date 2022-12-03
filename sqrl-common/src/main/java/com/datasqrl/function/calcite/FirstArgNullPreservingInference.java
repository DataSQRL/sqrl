package com.datasqrl.function.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Null preserving
 */
public class FirstArgNullPreservingInference implements SqlReturnTypeInference {

  private final RelProtoDataType proto;

  public FirstArgNullPreservingInference(RelProtoDataType proto) {
    this.proto = proto;
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if (opBinding.getOperandCount() == 0) {
      //No args then assume nonnullable
      return createTypeFromProto(opBinding.getTypeFactory(), false);
    }

    RelDataType operandType = opBinding.getOperandType(0);
    return createTypeFromProto(opBinding.getTypeFactory(), operandType.isNullable());
  }

  private RelDataType createTypeFromProto(RelDataTypeFactory typeFactory, boolean nullable) {
    return typeFactory.createTypeWithNullability(proto.apply(typeFactory), nullable);
  }
}