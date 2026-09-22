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
package com.datasqrl.server.util;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SqlTypeConverter {

  /**
   * Maps a SQL type name from a Calcite {@code RelDataType} to the corresponding Java class.
   *
   * @param sqlTypeName the SQL type name as returned by {@code
   *     RelDataType#getSqlTypeName().name()};
   * @return the corresponding Java class, or {@code null} if {@code sqlTypeName} is {@code null}
   */
  public static Class<?> sqlTypeNameToJavaClass(String sqlTypeName) {
    if (sqlTypeName == null) {
      return null;
    }

    return switch (sqlTypeName) {
      case "INTEGER" -> Integer.class;
      case "BIGINT" -> Long.class;
      case "SMALLINT" -> Short.class;
      case "TINYINT" -> Byte.class;
      case "FLOAT", "REAL" -> Float.class;
      case "DOUBLE" -> Double.class;
      case "DECIMAL" -> BigDecimal.class;
      case "BOOLEAN" -> Boolean.class;
      case "DATE" -> LocalDate.class;
      case "TIME" -> LocalTime.class;
      case "TIME_WITH_LOCAL_TIME_ZONE" -> OffsetTime.class;
      case "TIMESTAMP" -> LocalDateTime.class;
      case "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "TIMESTAMP_WITH_TIME_ZONE" -> OffsetDateTime.class;
      case "CHAR", "VARCHAR" -> String.class;
      default -> String.class;
    };
  }

  /** Converts a JSON-compatible parameter value to the Java type expected for the SQL type. */
  public static Object convert(Object value, String sqlTypeName) {
    if (value == null || sqlTypeName == null) {
      return value;
    }

    return switch (sqlTypeName) {
      case "INTEGER" -> number(value).intValue();
      case "BIGINT" -> number(value).longValue();
      case "SMALLINT" -> number(value).shortValue();
      case "TINYINT" -> number(value).byteValue();
      case "FLOAT", "REAL" -> number(value).floatValue();
      case "DOUBLE" -> number(value).doubleValue();
      case "DECIMAL" -> value instanceof BigDecimal ? value : new BigDecimal(value.toString());
      case "BOOLEAN" -> value instanceof Boolean ? value : Boolean.valueOf(value.toString());
      case "DATE" -> value instanceof LocalDate ? value : LocalDate.parse(value.toString());
      case "TIME" -> value instanceof LocalTime ? value : LocalTime.parse(value.toString());
      case "TIME_WITH_LOCAL_TIME_ZONE" ->
          value instanceof OffsetTime ? value : OffsetTime.parse(value.toString());
      case "TIMESTAMP" -> toLocalDateTime(value);
      case "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "TIMESTAMP_WITH_TIME_ZONE" -> toOffsetDateTime(value);
      case "CHAR", "VARCHAR" -> value.toString();
      default -> value;
    };
  }

  private static Number number(Object value) {
    return value instanceof Number number ? number : new BigDecimal(value.toString());
  }

  private static LocalDateTime toLocalDateTime(Object value) {
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime;
    }
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime.toLocalDateTime();
    }
    return LocalDateTime.parse(value.toString());
  }

  private static OffsetDateTime toOffsetDateTime(Object value) {
    if (value instanceof OffsetDateTime offsetDateTime) {
      return offsetDateTime;
    }
    if (value instanceof LocalDateTime localDateTime) {
      return localDateTime.atOffset(ZoneOffset.UTC);
    }
    try {
      return OffsetDateTime.parse(value.toString());
    } catch (DateTimeParseException e) {
      return LocalDateTime.parse(value.toString()).atOffset(ZoneOffset.UTC);
    }
  }
}
