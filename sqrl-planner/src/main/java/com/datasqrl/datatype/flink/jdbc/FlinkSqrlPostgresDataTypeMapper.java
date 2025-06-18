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
package com.datasqrl.datatype.flink.jdbc;

import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.DataTypeMappings;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
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
    if (type.getSqlTypeName() == SqlTypeName.MAP
        || type.getSqlTypeName() == SqlTypeName.ROW
        || type.getSqlTypeName() == SqlTypeName.ARRAY) {
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
