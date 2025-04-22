package com.datasqrl.datatype.flink.csv;

import java.util.Optional;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.google.auto.service.AutoService;

@AutoService(DataTypeMapper.class)
public class FlexibleCsvFlinkFormatTypeMapper extends FlinkDataTypeMapper {

  @Override
public boolean nativeTypeSupport(RelDataType type) {
    return switch (type.getSqlTypeName()) {
    case REAL, NULL, SYMBOL, DISTINCT, STRUCTURED, OTHER, CURSOR, COLUMN_LIST, DYNAMIC_STAR, GEOMETRY, SARG -> false;
    default -> false;
    case TINYINT, MULTISET, INTERVAL_YEAR, INTERVAL_YEAR_MONTH, INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND, INTERVAL_HOUR, INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND,
            INTERVAL_MINUTE, INTERVAL_MINUTE_SECOND, INTERVAL_SECOND, BOOLEAN, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, DOUBLE, DATE, TIME,
            TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, CHAR, VARCHAR, BINARY, VARBINARY, MAP, ROW ->
        true;
    case ANY -> false;
    case ARRAY -> nativeTypeSupport(type.getComponentType());
    };
  }

  @Override
  public Optional<CastFunction> convertType(RelDataType type) {
    if (nativeTypeSupport(type)) {
      return Optional.empty(); //no cast needed
    }

    // Cast needed, convert to bytes
    return Optional.of(
        new CastFunction(SerializeToBytes.class.getName(),
            convert(new SerializeToBytes())));
  }

  @Override
  public boolean isTypeOf(TableConfig tableConfig) {
    return tableConfig.getConnectorConfig().getFormat().map(
        format -> format.equalsIgnoreCase("flexible-csv")
    ).orElse(false);
  }
}
