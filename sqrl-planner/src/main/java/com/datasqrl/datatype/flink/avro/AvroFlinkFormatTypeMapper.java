package com.datasqrl.datatype.flink.avro;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.json.JsonToString;
import com.datasqrl.vector.FlinkVectorType;
import com.datasqrl.vector.VectorToDouble;
import com.google.auto.service.AutoService;

@AutoService(DataTypeMapper.class)
public class AvroFlinkFormatTypeMapper extends FlinkDataTypeMapper {

  @Override
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
  public boolean isTypeOf(TableConfig tableConfig) {
    var formatOpt = tableConfig.getConnectorConfig().getFormat();
    if (formatOpt.isEmpty()) {
      return false;
    }

    var format = formatOpt.get();


    return format.getName().equalsIgnoreCase("avro") || format.getName()
        .equalsIgnoreCase("avro-confluent");
  }
}
