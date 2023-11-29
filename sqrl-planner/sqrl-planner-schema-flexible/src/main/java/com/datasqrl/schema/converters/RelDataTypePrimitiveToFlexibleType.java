package com.datasqrl.schema.converters;

import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.TimestampType;
import com.datasqrl.schema.type.basic.DoubleType;
import com.datasqrl.schema.type.basic.BigIntType;
import com.datasqrl.schema.type.basic.StringType;
import org.apache.calcite.rel.type.RelDataType;

public class RelDataTypePrimitiveToFlexibleType {

  public static Type toType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return new BooleanType();
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
      case DATE:
      case TIMESTAMP:
        return new BigIntType();
      case CHAR:
      case VARCHAR:
        return new StringType();
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return new DoubleType();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIME:
        return new TimestampType();
      case BINARY:
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY:
      case NULL:
      case SYMBOL:
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
