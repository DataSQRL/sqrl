package com.datasqrl.datatype.flink.iceberg;

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
import com.datasqrl.flinkrunner.functions.json.jsonb_to_string;
import com.datasqrl.flinkrunner.functions.vector.vector_to_double;
import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import com.datasqrl.flinkrunner.types.vector.FlinkVectorType;
import com.google.auto.service.AutoService;

@AutoService(DataTypeMapper.class)
public class IcebergDataTypeMapper extends FlinkDataTypeMapper implements DataTypeMapping {


  @Override
  public Optional<Mapper> getMapper(RelDataType type) {
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
      case MULTISET:
      case MAP:
      case ROW:
        return Optional.empty();
      case ARRAY:
        if (getMapper(type.getComponentType()).isEmpty()) {
            return Optional.empty();
        }
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
    case ROW: //todo iterate over the row
        yield true;
    case ARRAY:
        yield nativeTypeSupport(type.getComponentType());
    };
  }

  @Override
  public Optional<CastFunction> convertType(RelDataType type) {
    if (nativeTypeSupport(type)) {
      return Optional.empty(); //no cast needed
    }

    // Explicit downcast
    if (type instanceof RawRelDataType rawRelDataType) {
      if (rawRelDataType.getRawType().getDefaultConversion() == FlinkJsonType.class) {
        return Optional.of(
            new CastFunction(jsonb_to_string.class.getName(),
                convert(new jsonb_to_string())));
      } else if (rawRelDataType.getRawType().getDefaultConversion() == FlinkVectorType.class) {
        return Optional.of(
            new CastFunction(vector_to_double.class.getName(),
                convert(new vector_to_double())));
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(
        new CastFunction(SerializeToBytes.class.getName(),
            convert(new SerializeToBytes())));
  }

  @Override
  public boolean isTypeOf(TableConfig tableConfig) {
    var connectorNameOpt = tableConfig.getConnectorConfig().getConnectorName();
    if (connectorNameOpt.isEmpty()) {
      return false;
    }

    var connectorName = connectorNameOpt.get();
    return connectorName.equalsIgnoreCase("iceberg");
  }
}
