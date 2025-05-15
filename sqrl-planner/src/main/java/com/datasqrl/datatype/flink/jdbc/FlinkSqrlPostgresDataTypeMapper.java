package com.datasqrl.datatype.flink.jdbc;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import com.datasqrl.flinkrunner.types.vector.FlinkVectorType;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

public class FlinkSqrlPostgresDataTypeMapper implements DataTypeMapping {

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
    if (type instanceof RawRelDataType rawRelDataType) {
      Class clazz = rawRelDataType.getRawType().getDefaultConversion();
      if (clazz == FlinkJsonType.class || clazz == FlinkVectorType.class) {
        return Optional.empty();
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(DataTypeMappings.TO_BYTES_ONLY);
  }
}
