package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.io.schema.flexible.type.ArrayType;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BooleanType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
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
      case MULTISET:
        return new ArrayType(toType(type.getComponentType()));
      case BINARY:
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY:
      case NULL:
      case SYMBOL:
      case ARRAY:
      case MAP:
      case ROW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }
}
