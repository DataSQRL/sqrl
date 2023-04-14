package com.datasqrl.plan.local.analyze;

import com.datasqrl.schema.UniversalTable.Column;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.BooleanType;
import com.datasqrl.schema.type.basic.DateTimeType;
import com.datasqrl.schema.type.basic.FloatType;
import com.datasqrl.schema.type.basic.IntegerType;
import com.datasqrl.schema.type.basic.StringType;
import com.datasqrl.schema.type.basic.UuidType;

public class UtbTypeToFlexibleType {

  public static Type toType(Column type) {
    switch (type.getType().getSqlTypeName()) {
      case BOOLEAN:
        return new BooleanType();
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
      case DATE:
      case TIMESTAMP:
        return new IntegerType();
      case CHAR:
      case VARCHAR:
        if (type.getType().getPrecision() == 32) {
          return new UuidType();
        }
        return new StringType();
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return new FloatType();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case TIME:
        return new DateTimeType();
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
