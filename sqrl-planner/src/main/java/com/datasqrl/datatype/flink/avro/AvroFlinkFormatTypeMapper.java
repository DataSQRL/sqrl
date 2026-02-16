/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.datatype.flink.avro;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

public class AvroFlinkFormatTypeMapper implements DataTypeMapping {

  public static final String FORMAT_NAME = "avro";

  @Override
  public Optional<Mapper> getMapper(RelDataType type) {
    // These are the supported types
    switch (type.getSqlTypeName()) {
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
      case TIMESTAMP:
      case CHAR:
      case VARCHAR:
      case BINARY:
      case VARBINARY:
      case MAP:
      case MULTISET:
        return Optional.empty();
      case ARRAY:
        if (getMapper(type.getComponentType()).isEmpty()) {
          return Optional.empty();
        }
    }
    if (type.getSqlTypeName() == SqlTypeName.ROW
        || (type.getSqlTypeName() == SqlTypeName.ARRAY
            && type.getComponentType().getSqlTypeName() == SqlTypeName.ROW)) {
      return Optional.of(DataTypeMappings.TO_JSON_ONLY);
    }

    if (type instanceof RawRelDataType rawRelDataType) {
      if (rawRelDataType.getRawType().getDefaultConversion() == FlinkVectorType.class) {
        return Optional.of(DataTypeMappings.VECTOR_TO_DOUBLE_ONLY);
      } else if (rawRelDataType.getRawType().getDefaultConversion() == FlinkJsonType.class) {
        return Optional.of(DataTypeMappings.JSON_STRING);
      }
    }

    // Cast needed, convert to bytes
    return Optional.of(DataTypeMappings.TO_BYTES_ONLY);
  }
}
