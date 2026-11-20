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
package com.datasqrl.calcite.type;

import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

/**
 * The kinds of types that the compiler maps to other type systems, such as GraphQL or data
 * contracts. The kinds are as fine-grained as any of those mappings needs.
 */
public enum TypeKind {
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  /** FLOAT and REAL */
  FLOAT,
  DOUBLE,
  DATE,
  TIME,
  TIME_LTZ,
  TIMESTAMP,
  TIMESTAMP_LTZ,
  /** All interval types */
  INTERVAL,
  /** CHAR and VARCHAR */
  STRING,
  /** BINARY and VARBINARY */
  BINARY,
  JSON,
  VECTOR,
  /** ROW and STRUCTURED */
  ROW,
  /** ARRAY and MULTISET */
  ARRAY,
  MAP,
  /** Types that have no mapping, such as NULL, ANY, SYMBOL and other RAW types */
  UNSUPPORTED;

  public static TypeKind of(RelDataType type) {
    var typeName = type.getSqlTypeName();
    if (typeName == null) {
      return UNSUPPORTED;
    }
    if (SqlTypeName.INTERVAL_TYPES.contains(typeName)) {
      return INTERVAL;
    }

    return switch (typeName) {
      case BOOLEAN -> BOOLEAN;
      case TINYINT -> TINYINT;
      case SMALLINT -> SMALLINT;
      case INTEGER -> INTEGER;
      case BIGINT -> BIGINT;
      case DECIMAL -> DECIMAL;
      case FLOAT, REAL -> FLOAT;
      case DOUBLE -> DOUBLE;
      case DATE -> DATE;
      case TIME -> TIME;
      case TIME_WITH_LOCAL_TIME_ZONE -> TIME_LTZ;
      case TIMESTAMP -> TIMESTAMP;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> TIMESTAMP_LTZ;
      case CHAR, VARCHAR -> STRING;
      case BINARY, VARBINARY -> BINARY;
      case ROW, STRUCTURED -> ROW;
      case ARRAY, MULTISET -> ARRAY;
      case MAP -> MAP;
      case OTHER -> ofRawType(type);
      default -> UNSUPPORTED;
    };
  }

  private static TypeKind ofRawType(RelDataType type) {
    if (!(type instanceof RawRelDataType rawType)) {
      return UNSUPPORTED;
    }

    var originatingClass = rawType.getRawType().getOriginatingClass();
    if (originatingClass.isAssignableFrom(FlinkJsonType.class)) {
      return JSON;
    }
    if (originatingClass.isAssignableFrom(FlinkVectorType.class)) {
      return VECTOR;
    }
    return UNSUPPORTED;
  }
}
