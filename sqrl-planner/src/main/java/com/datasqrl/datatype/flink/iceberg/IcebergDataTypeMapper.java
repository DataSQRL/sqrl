package com.datasqrl.datatype.flink.iceberg;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.flinkrunner.types.json.FlinkJsonType;
import com.datasqrl.flinkrunner.types.vector.FlinkVectorType;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

public class IcebergDataTypeMapper implements DataTypeMapping {


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

}
