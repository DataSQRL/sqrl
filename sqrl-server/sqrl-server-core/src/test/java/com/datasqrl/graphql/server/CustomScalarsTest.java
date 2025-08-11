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
package com.datasqrl.graphql.server;

import static com.datasqrl.graphql.server.CustomScalars.FlexibleDateTimeCoercing.normalizeTimestamp;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CustomScalarsTest {

  @Test
  void givenFullRFC3339Timestamp_whenNormalizeTimestamp_thenReturnsSameFormat() {
    var input = "2025-05-22T15:30:45.123Z";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45.123Z");
  }

  @Test
  void givenFlinkShortTimestamp_whenNormalizeTimestamp_thenReturnsRFC3339Format() {
    var input = "2025-05-22T15:30";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:00Z");
  }

  @Test
  void givenTimestampWithSeconds_whenNormalizeTimestamp_thenReturnsRFC3339Format() {
    var input = "2025-05-22T15:30:45";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45Z");
  }

  @Test
  void givenTimestampWithMilliseconds_whenNormalizeTimestamp_thenReturnsRFC3339Format() {
    var input = "2025-05-22T15:30:45.123";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45.123Z");
  }

  @Test
  void givenTimestampWithMicroseconds_whenNormalizeTimestamp_thenReturnsRFC3339Format() {
    var input = "2025-05-22T15:30:45.123456";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45.123456Z");
  }

  @Test
  void givenTimestampWithNanoseconds_whenNormalizeTimestamp_thenReturnsRFC3339Format() {
    var input = "2025-05-22T15:30:45.123456789";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45.123456789Z");
  }

  @Test
  void givenTimestampWithOffset_whenNormalizeTimestamp_thenPreservesOffset() {
    var input = "2025-05-22T15:30:45+02:00";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45+02:00");
  }

  @Test
  void givenTimestampWithZOffset_whenNormalizeTimestamp_thenPreservesZOffset() {
    var input = "2025-05-22T15:30:45Z";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo("2025-05-22T15:30:45Z");
  }

  @Test
  void givenNullInput_whenNormalizeTimestamp_thenReturnsNull() {
    var result = normalizeTimestamp(null);
    assertThat(result).isNull();
  }

  @Test
  void givenEmptyvar_whenNormalizeTimestamp_thenReturnsEmptyvar() {
    var result = normalizeTimestamp("");
    assertThat(result).isEmpty();
  }

  @Test
  void givenWhitespaceOnlyvar_whenNormalizeTimestamp_thenReturnsOriginalvar() {
    var input = "   ";
    var result = normalizeTimestamp(input);
    assertThat(result).isEqualTo(input);
  }

  @Test
  void givenInvalidTimestamp_whenNormalizeTimestamp_thenReturnsNull() {
    var input = "invalid-timestamp";
    var result = normalizeTimestamp(input);
    assertThat(result).isNull();
  }

  @Test
  void givenInvalidDateFormat_whenNormalizeTimestamp_thenReturnsNull() {
    var input = "2025-13-22T15:30:45";
    var result = normalizeTimestamp(input);
    assertThat(result).isNull();
  }

  @Test
  void givenMissingDatePart_whenNormalizeTimestamp_thenReturnsNull() {
    var input = "T15:30:45";
    var result = normalizeTimestamp(input);
    assertThat(result).isNull();
  }

  @Test
  void givenMissingTimePart_whenNormalizeTimestamp_thenReturnsNull() {
    var input = "2025-05-22";
    var result = normalizeTimestamp(input);
    assertThat(result).isNull();
  }
}
