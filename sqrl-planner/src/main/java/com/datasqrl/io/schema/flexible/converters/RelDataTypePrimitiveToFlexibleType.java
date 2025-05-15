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
package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.io.schema.flexible.type.ArrayType;
import com.datasqrl.io.schema.flexible.type.Type;
import com.datasqrl.io.schema.flexible.type.basic.BigIntType;
import com.datasqrl.io.schema.flexible.type.basic.BooleanType;
import com.datasqrl.io.schema.flexible.type.basic.DoubleType;
import com.datasqrl.io.schema.flexible.type.basic.StringType;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;
import org.apache.calcite.rel.type.RelDataType;

public class RelDataTypePrimitiveToFlexibleType {

  public static Type toType(RelDataType type) {
    return switch (type.getSqlTypeName()) {
      case BOOLEAN -> new BooleanType();
      case TINYINT, SMALLINT, BIGINT, INTEGER, DATE, TIMESTAMP -> new BigIntType();
      case CHAR, VARCHAR -> new StringType();
      case DECIMAL, FLOAT, DOUBLE -> new DoubleType();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME -> new TimestampType();
      case MULTISET -> new ArrayType(toType(type.getComponentType()));
      case BINARY, VARBINARY, INTERVAL_YEAR_MONTH, INTERVAL_DAY, NULL, SYMBOL, ARRAY, MAP, ROW ->
          throw new UnsupportedOperationException("Unsupported type:" + type);
      default -> throw new UnsupportedOperationException("Unsupported type:" + type);
    };
  }
}
