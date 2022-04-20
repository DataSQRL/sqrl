package ai.datasqrl.schema.converters;

import ai.datasqrl.schema.type.basic.BasicType;
import ai.datasqrl.schema.type.basic.BigIntegerType;
import ai.datasqrl.schema.type.basic.BooleanType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.DoubleType;
import ai.datasqrl.schema.type.basic.FloatType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.IntervalType;
import ai.datasqrl.schema.type.basic.NullType;
import ai.datasqrl.schema.type.basic.StringType;
import ai.datasqrl.schema.type.basic.UuidType;
import org.apache.calcite.rel.type.RelDataType;

public class CalciteToSqrlTypeConverter {

  public static BasicType toBasicType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return BooleanType.INSTANCE;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return IntegerType.INSTANCE;
      case BIGINT:
        return BigIntegerType.INSTANCE;
      case DECIMAL:
      case FLOAT:
        return FloatType.INSTANCE;
      case REAL:
      case DOUBLE:
        return DoubleType.INSTANCE;
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return DateTimeType.INSTANCE;
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return IntervalType.INSTANCE;
      case CHAR:
      case VARCHAR:
        return StringType.INSTANCE;
      case NULL:
        return NullType.INSTANCE;
      case VARBINARY:
        return UuidType.INSTANCE;
      case BINARY:
      case ANY:
      case SYMBOL:
      case MULTISET:
      case ARRAY:
      case MAP:
      case DISTINCT:
      case STRUCTURED:
      case ROW:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
    }
    
    throw new RuntimeException("Unknown column:" + type) ;
  }
}
