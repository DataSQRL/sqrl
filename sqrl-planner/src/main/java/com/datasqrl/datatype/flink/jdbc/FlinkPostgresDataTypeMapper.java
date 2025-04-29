package com.datasqrl.datatype.flink.jdbc;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.flinkrunner.functions.json.jsonb_to_string;
import com.datasqrl.flinkrunner.functions.vector.vector_to_double;
import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import com.datasqrl.flinkrunner.types.vector.FlinkVectorType;
import com.google.auto.service.AutoService;

@AutoService(DataTypeMapper.class)
public class FlinkPostgresDataTypeMapper extends FlinkDataTypeMapper {

  @Override
public boolean nativeTypeSupport(RelDataType type) {
    return switch (type.getSqlTypeName()) {
    case TINYINT, REAL, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND,
            INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND, NULL, SYMBOL, MULTISET, DISTINCT, STRUCTURED, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR,
            GEOMETRY, SARG, ANY ->
        false;
    default -> false;
    case BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, DOUBLE, DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            CHAR, VARCHAR, BINARY, VARBINARY ->
        true;
    case ARRAY -> false;
    case MAP -> false;
    case ROW -> false;
    };
  }

  @Override
  public Optional<CastFunction> convertType(RelDataType type) {
    if (nativeTypeSupport(type)) {
      return Optional.empty(); //no cast needed
    }

    // Explicit downcast for json
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
    if (!connectorName.equalsIgnoreCase("jdbc")) {
      return false;
    }

    var url = (String)tableConfig.getConnectorConfig().toMap().get("url");
    return url.toLowerCase().startsWith("jdbc:postgresql:");
  }
}
