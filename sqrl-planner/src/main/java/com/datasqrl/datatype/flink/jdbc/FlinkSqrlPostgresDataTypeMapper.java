package com.datasqrl.datatype.flink.jdbc;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.config.TableConfig;
import com.datasqrl.datatype.DataTypeMapper;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.datatype.SerializeToBytes;
import com.datasqrl.datatype.flink.FlinkDataTypeMapper;
import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.json.ToJson;
import com.datasqrl.vector.FlinkVectorType;
import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@AutoService(DataTypeMapper.class)
public class FlinkSqrlPostgresDataTypeMapper extends FlinkDataTypeMapper implements
    DataTypeMapping {

  @Override
  public Optional<Mapper> getMapper(RelDataType type) {
    switch (type.getSqlTypeName()) {
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
        return Optional.empty();

    }
    if (type.getSqlTypeName() == SqlTypeName.MAP ||
        type.getSqlTypeName() == SqlTypeName.ROW || type.getSqlTypeName() == SqlTypeName.ARRAY) {
      return Optional.of(DataTypeMappings.TO_JSON_ONLY);
    }
    if (type instanceof RawRelDataType) {
      RawRelDataType rawRelDataType = (RawRelDataType) type;
      Class clazz = rawRelDataType.getRawType().getDefaultConversion();
      if (clazz == FlinkJsonType.class || clazz == FlinkVectorType.class) {
        return Optional.empty();
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(DataTypeMappings.TO_BYTES_ONLY);
  }

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
        return false;
      case MAP:
        return false;
      case ROW:
        return false;
    }
  }

  @Override
  public Optional<CastFunction> convertType(RelDataType type) {

    if (type.getSqlTypeName() == SqlTypeName.MAP ||
        type.getSqlTypeName() == SqlTypeName.ROW || type.getSqlTypeName() == SqlTypeName.ARRAY) {
      return Optional.of(new CastFunction(ToJson.class.getName(), convert(new ToJson())));
    }

    // Cast needed, convert to bytes
    return Optional.of(
        new CastFunction(SerializeToBytes.class.getName(),
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
