package com.datasqrl.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class TypeUtil {

  public static RelDataType makeTimestampType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
  }

  public static RelDataType makeTimestampType(RelDataTypeFactory typeFactory, boolean nullable) {
    return withNullable(typeFactory, makeTimestampType(typeFactory), nullable);
  }

  public static RelDataType makeUuidType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.CHAR, 36);
  }

  public static RelDataType makeUuidType(RelDataTypeFactory typeFactory, boolean nullable) {
    return withNullable(typeFactory, makeUuidType(typeFactory), nullable);
  }

  public static RelDataType withNullable(RelDataTypeFactory typeFactory, RelDataType type, boolean nullable) {
    return typeFactory.createTypeWithNullability(type, nullable);
  }

}
