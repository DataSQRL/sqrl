package com.datasqrl.schema.converters;

import com.datasqrl.schema.UniversalTable.Column;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.IntegerType;
import com.datasqrl.schema.type.basic.TimestampType;
import com.datasqrl.schema.type.basic.DoubleType;
import com.datasqrl.schema.type.basic.BigIntType;
import com.datasqrl.schema.type.basic.StringType;

public class UtbTypeToFlexibleType {

  public static Type toType(Column type) {
    switch (type.getType().getSqlTypeName()) {
      case BOOLEAN:
        return new BooleanType();
      case DATE:
      case TIMESTAMP:
      case BIGINT:
        return new BigIntType();
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return new IntegerType();
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
