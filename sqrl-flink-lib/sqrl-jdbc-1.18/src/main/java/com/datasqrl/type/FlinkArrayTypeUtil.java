package com.datasqrl.type;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkArrayTypeUtil {

  public static LogicalType getBaseFlinkArrayType(LogicalType type) {
    if (type instanceof ArrayType) {
      return getBaseFlinkArrayType(((ArrayType) type).getElementType());
    }
    return type;
  }

  public static boolean isScalarArray(LogicalType type) {
    if (type instanceof ArrayType) {
      LogicalType elementType = ((ArrayType) type).getElementType();
      return isScalar(elementType) || isScalarArray(elementType);
    }
    return false;
  }

  public static boolean isScalar(LogicalType type) {
    switch (type.getTypeRoot()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

}
