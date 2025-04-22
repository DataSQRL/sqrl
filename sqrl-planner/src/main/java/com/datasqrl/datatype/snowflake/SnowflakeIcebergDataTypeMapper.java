package com.datasqrl.datatype.snowflake;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.types.json.FlinkJsonType;
import com.google.auto.service.AutoService;

/** We should only map at the table format engine level, the rest should be function translations */
@AutoService(DataTypeMapper.class)
@Deprecated
public class SnowflakeIcebergDataTypeMapper implements DataTypeMapper {

  @Override
  public String getEngineName() {
    return "snowflake";
  }

  @Override
  public boolean nativeTypeSupport(RelDataType type) {
    return switch (type.getSqlTypeName()) {
    case REAL:
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
    case NULL:
    case SYMBOL:
    case DISTINCT:
    case STRUCTURED:
    case OTHER:
    case CURSOR:
    case COLUMN_LIST:
    case DYNAMIC_STAR:
    case GEOMETRY:
    case SARG:
    case ANY:
    default:
        yield false;
    case TINYINT:
    case BOOLEAN:
    case SMALLINT:
    case INTEGER:
    case BIGINT:
    case DECIMAL:
    case FLOAT:
    case DOUBLE:
    case DATE:
    case TIME:
    case TIME_WITH_LOCAL_TIME_ZONE:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case CHAR:
    case VARCHAR:
    case BINARY:
    case VARBINARY:
    case MULTISET:
    case MAP:
    case ROW: // todo iterate over the row
        yield true;
    case ARRAY:
        yield nativeTypeSupport(type.getComponentType());
    };
  }

  @Override
  public Optional<CastFunction> convertType(RelDataType type) {
    // Explicit downcast
    if (type instanceof RawRelDataType rawRelDataType) {
      if (rawRelDataType.getRawType().getDefaultConversion() == FlinkJsonType.class) {
        throw new RuntimeException("Writing json to snowflake not yet supported");
      }
    }

    return Optional.empty(); // Could not create type
  }
}
