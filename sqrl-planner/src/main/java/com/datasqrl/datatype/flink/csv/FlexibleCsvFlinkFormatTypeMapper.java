package com.datasqrl.datatype.flink.csv;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

@AutoService(DataTypeMapper.class)
public class FlexibleCsvFlinkFormatTypeMapper extends FlinkDataTypeMapper {

  public boolean nativeTypeSupport(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case REAL:
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
      default:
        return false;
      case TINYINT:
      case MULTISET:
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
      case ROW:
        return true;
      case ANY:
        return false;
      case ARRAY:
        return nativeTypeSupport(type.getComponentType());
    }
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
