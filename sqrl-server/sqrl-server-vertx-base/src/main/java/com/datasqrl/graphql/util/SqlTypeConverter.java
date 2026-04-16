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
package com.datasqrl.graphql.util;

import java.math.BigDecimal;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SqlTypeConverter {

  /**
   * Maps a SQL type name from a Calcite {@code RelDataType} to the corresponding built-in Java
   * class. Unrecognised types (including date-time and character types) fall through to {@link
   * String}.
   *
   * @param sqlTypeName the SQL type name as returned by {@code
   *     RelDataType#getSqlTypeName().name()};
   * @return the corresponding Java class, or {@code null} if {@code sqlTypeName} is {@code null}
   */
  public static Class<?> sqlTypeNameToJavaClass(String sqlTypeName) {
    if (sqlTypeName == null) {
      return null;
    }

    // TODO: Cover date-time/timestamp types if necessary.
    return switch (sqlTypeName) {
      case "INTEGER" -> Integer.class;
      case "BIGINT" -> Long.class;
      case "SMALLINT" -> Short.class;
      case "TINYINT" -> Byte.class;
      case "FLOAT", "REAL" -> Float.class;
      case "DOUBLE" -> Double.class;
      case "DECIMAL" -> BigDecimal.class;
      case "BOOLEAN" -> Boolean.class;
      default -> String.class;
    };
  }
}
