package com.datasqrl.datatype.flink.avro;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.types.json.FlinkJsonType;
import com.datasqrl.types.json.functions.JsonToString;
import com.datasqrl.types.vector.FlinkVectorType;
import com.datasqrl.types.vector.functions.VectorToDouble;
import com.google.auto.service.AutoService;

@AutoService(DataTypeMapper.class)
public class AvroFlinkFormatTypeMapper extends FlinkDataTypeMapper implements DataTypeMapping {

  @Override
  public Optional<Mapper> getMapper(RelDataType type) {
    //These are the supported types
    switch (type.getSqlTypeName()) {
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
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
        return Optional.empty(); //These are supported
    }
    if (type instanceof RawRelDataType rawRelDataType) {
      if (rawRelDataType.getRawType().getDefaultConversion() == FlinkJsonType.class) {
        return Optional.of(DataTypeMappings.JSON_TO_STRING_ONLY);
      } else if (rawRelDataType.getRawType().getDefaultConversion() == FlinkVectorType.class) {
        return Optional.of(DataTypeMappings.VECTOR_TO_DOUBLE_ONLY);
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(DataTypeMappings.TO_BYTES_ONLY);
  }

  @Override
@Deprecated
  public boolean nativeTypeSupport(RelDataType type) {
    return switch (type.getSqlTypeName()) {
    case REAL, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE,
            INTERVAL_MINUTE_SECOND, INTERVAL_SECOND, NULL, SYMBOL, DISTINCT, STRUCTURED, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR, GEOMETRY,
            SARG ->
        false;
    default -> false;
    case TINYINT, BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, DOUBLE, DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP,
            TIMESTAMP_WITH_LOCAL_TIME_ZONE, CHAR, VARCHAR, BINARY, VARBINARY, ARRAY, MAP, MULTISET, ROW ->
        true;
    case ANY -> false;
    };
  }

  @Override
  @Deprecated
  public Optional<CastFunction> convertType(RelDataType type) {
    if (nativeTypeSupport(type)) {
      return Optional.empty(); //no cast needed
    }

    if (type instanceof RawRelDataType rawRelDataType) {
      if (rawRelDataType.getRawType().getDefaultConversion() == FlinkJsonType.class) {
        return Optional.of(
            new CastFunction(JsonToString.class.getName(),
                convert(new JsonToString())));
      } else if (rawRelDataType.getRawType().getDefaultConversion() == FlinkVectorType.class) {
        return Optional.of(
            new CastFunction(VectorToDouble.class.getName(),
                convert(new VectorToDouble())));
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(
        new CastFunction(SerializeToBytes.class.getName(),
            convert(new SerializeToBytes())));
  }

  @Override
  @Deprecated
  public boolean isTypeOf(TableConfig tableConfig) {
    return tableConfig.getConnectorConfig().getFormat().map(
        format -> format.equalsIgnoreCase("avro") ||
            format.equalsIgnoreCase("avro-confluent")
    ).orElse(false);
  }

}
