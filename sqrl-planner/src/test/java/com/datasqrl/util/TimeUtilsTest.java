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
package com.datasqrl.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TimeUtilsTest {

  @ParameterizedTest
  @CsvSource({
    "30 min, PT30M",
    "45 minutes, PT45M",
    "36 hours, PT36H",
    "1 h, PT1H",
    "14 days, PT336H",
    "1 d, PT24H",
    "14days, PT336H",
    "10 s, PT10S",
    "500 ms, PT0.5S",
    "500, PT0.5S"
  })
  void givenDurationString_whenParseDuration_thenParsed(String argument, Duration expected) {
    assertThat(TimeUtils.parseDuration(argument)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "30 min, MINUTES",
    "45 minutes, MINUTES",
    "1 m, MINUTES",
    "36 hours, HOURS",
    "1 h, HOURS",
    "14 days, DAYS",
    "1 d, DAYS",
    "14days, DAYS",
    "10 s, SECONDS",
    "500 ms, MILLIS",
    "500, MILLIS"
  })
  void givenDurationString_whenParseDurationUnit_thenUnitExtracted(
      String argument, ChronoUnit expected) {
    assertThat(TimeUtils.parseDurationUnit(argument)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"2 weeks", "3 months", "1 year", "fortnight", "days"})
  void givenUnsupportedUnitOrFormat_whenParseDurationUnit_thenThrows(String argument) {
    assertThatThrownBy(() -> TimeUtils.parseDurationUnit(argument))
        .isInstanceOfAny(IllegalArgumentException.class, NumberFormatException.class);
  }
}
