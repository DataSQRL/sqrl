package ai.datasqrl.schema.converters;

import ai.datasqrl.schema.type.basic.*;
import org.apache.calcite.rel.type.RelDataType;

public class CalciteToSqrlTypeConverter {

  public static BasicType toBasicType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return BooleanType.INSTANCE;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return IntegerType.INSTANCE;
      case DECIMAL:
      case FLOAT:
      case REAL:
      case DOUBLE:
        return FloatType.INSTANCE;
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
      case VARBINARY:
        return UuidType.INSTANCE;
      case BINARY:
      case NULL:
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
      case SARG:
    }

    throw new RuntimeException("Unknown column:" + type);
  }
}
