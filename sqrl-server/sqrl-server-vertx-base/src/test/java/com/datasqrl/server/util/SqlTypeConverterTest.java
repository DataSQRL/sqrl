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

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SqlTypeConverterTest {

  @ParameterizedTest
  @MethodSource("scalarValues")
  void convertsRegisteredSqlTypes(String sqlType, Object value, Object expected) {
    assertThat(SqlTypeConverter.convert(value, sqlType)).isEqualTo(expected);
  }

  private static Stream<Arguments> scalarValues() {
    return Stream.of(
        Arguments.of("INTEGER", "42", 42),
        Arguments.of("BIGINT", "42", 42L),
        Arguments.of("SMALLINT", "42", (short) 42),
        Arguments.of("TINYINT", "42", (byte) 42),
        Arguments.of("FLOAT", "4.2", 4.2F),
        Arguments.of("REAL", "4.2", 4.2F),
        Arguments.of("DOUBLE", "4.2", 4.2D),
        Arguments.of("DECIMAL", "4.20", new BigDecimal("4.20")),
        Arguments.of("BOOLEAN", "true", true),
        Arguments.of("CHAR", 42, "42"),
        Arguments.of("DATE", "2025-09-09", LocalDate.parse("2025-09-09")),
        Arguments.of("TIME", "10:15:30", LocalTime.parse("10:15:30")),
        Arguments.of(
            "TIME_WITH_LOCAL_TIME_ZONE", "10:15:30+02:00", OffsetTime.parse("10:15:30+02:00")),
        Arguments.of(
            "TIMESTAMP", "2025-09-09T10:15:30", LocalDateTime.parse("2025-09-09T10:15:30")),
        Arguments.of(
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
            "2025-09-09T10:15:30+02:00",
            OffsetDateTime.parse("2025-09-09T10:15:30+02:00")),
        Arguments.of(
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
            "2025-09-09T10:15:30",
            OffsetDateTime.parse("2025-09-09T10:15:30Z")));
  }
}
