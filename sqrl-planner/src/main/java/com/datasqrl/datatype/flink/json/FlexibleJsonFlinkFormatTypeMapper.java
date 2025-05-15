package com.datasqrl.datatype.flink.json;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import com.datasqrl.flinkrunner.types.vector.FlinkVectorType;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

public class FlexibleJsonFlinkFormatTypeMapper implements DataTypeMapping {

  @Override
  public Optional<Mapper> getMapper(RelDataType type) {
    //These are the supported types
    switch (type.getSqlTypeName()) {
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
      case MAP:
      case MULTISET:
        return Optional.empty();
      case ARRAY:
        if (getMapper(type.getComponentType()).isEmpty()) {
            return Optional.empty();
        }
    }
    if (type.getSqlTypeName() == SqlTypeName.ROW ||
        (type.getSqlTypeName() == SqlTypeName.ARRAY && type.getComponentType().getSqlTypeName() == SqlTypeName.ROW)) {
      return Optional.of(DataTypeMappings.TO_JSON_ONLY);
    }

    if (type instanceof RawRelDataType rawRelDataType) {
      if (rawRelDataType.getRawType().getDefaultConversion() == FlinkVectorType.class) {
        return Optional.of(DataTypeMappings.VECTOR_TO_DOUBLE_ONLY);
      } else if (rawRelDataType.getRawType().getDefaultConversion() == FlinkJsonType.class) {
        return Optional.empty();
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(DataTypeMappings.TO_BYTES_ONLY);
  }
}
