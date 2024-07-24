package com.datasqrl.datatype.flink.jdbc;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.vector.FlinkVectorType;
import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@AutoService(DataTypeMapper.class)
public class FlinkSqrlPostgresDataTypeMapper extends FlinkDataTypeMapper {

  public boolean nativeTypeSupport(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case TINYINT:
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
      case MULTISET:
      case DISTINCT:
      case STRUCTURED:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
      case ANY:
      default:
        return false;
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
        return true;
      case OTHER:
        if (type instanceof RawRelDataType) {
          RawRelDataType rawRelDataType = (RawRelDataType) type;
          Class clazz = rawRelDataType.getRawType().getDefaultConversion();
          if (clazz == FlinkJsonType.class || clazz == FlinkVectorType.class) {
            return true;
          }
        }
        return false;
      case ARRAY:
//        if (isScalar(type.getComponentType())) {
//          return supportsType(type.getComponentType());
//        }
        return true;
      case MAP:
        return false;
      case ROW:
        return true; //sqrl adds row to json conversion
    }
  }

  private boolean isScalar(RelDataType componentType) {
    switch (componentType.getSqlTypeName()) {
      case BOOLEAN:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
      case FLOAT:
      case DOUBLE:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case DATE:
      case TIMESTAMP:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }
  @Override
  public Optional<CastFunction> convertType(RelDataType type) {
    if (nativeTypeSupport(type)) {
      return Optional.empty(); //no cast needed
    }

//    if (type.getSqlTypeName() == SqlTypeName.ARRAY && !isScalar(type.getComponentType())) {
//      return Optional.of(new CastFunction(ToJson.class.getName(), convert(new ToJson())));
//    }
//
//    if (type instanceof RelRecordType) {
//      return Optional.of(new CastFunction(ToJson.class.getName(), convert(new ToJson())));
//    }

    // Cast needed, convert to bytes
    return Optional.of(
        new CastFunction(SerializeToBytes.class.getSimpleName(),
            convert(new SerializeToBytes())));
  }

  @Override
  public boolean isTypeOf(TableConfig tableConfig) {
    Optional<String> connectorNameOpt = tableConfig.getConnectorConfig().getConnectorName();
    if (connectorNameOpt.isEmpty()) {
      return false;
    }

    String connectorName = connectorNameOpt.get();
    if (!connectorName.equalsIgnoreCase("jdbc-sqrl")) {
      return false;
    }

    String driver = (String)tableConfig.getConnectorConfig().toMap().get("driver");
    if (driver == null) {
      return false;
    }
    return driver.toLowerCase().equalsIgnoreCase("org.postgresql.Driver");
  }
}
