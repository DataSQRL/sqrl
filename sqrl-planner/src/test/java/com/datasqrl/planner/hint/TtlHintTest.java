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
package com.datasqrl.planner.hint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.parser.StatementParserException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class TtlHintTest {

  private final TtlHint.TtlHintFactory ttlFactory = new TtlHint.TtlHintFactory();
  private final CacheHint.CacheHintFactory cacheFactory = new CacheHint.CacheHintFactory();

  private static ParsedObject<SqrlHint> hint(String name, String... args) {
    return new ParsedObject<>(new SqrlHint(name, List.of(args)), FileLocation.START);
  }

  @Test
  void givenSingleDurationArg_whenCreate_thenTtlAndUnitSet() {
    var hint = (TtlHint) ttlFactory.create(hint("ttl", "14 days"));

    assertThat(hint.getTtl()).contains(Duration.ofDays(14));
    assertThat(hint.getTtlUnit()).contains(ChronoUnit.DAYS);
  }

  @Test
  void givenNoArgs_whenCreate_thenEmptyTtlAndUnit() {
    var hint = (TtlHint) ttlFactory.create(hint("ttl"));

    assertThat(hint.getTtl()).isEmpty();
    assertThat(hint.getTtlUnit()).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({
    "30 min, 30, MINUTES",
    "45 minutes, 45, MINUTES",
    "36 hours, 2160, HOURS",
    "1 h, 60, HOURS",
    "14 days, 20160, DAYS",
    "1 d, 1440, DAYS",
    "2 weeks, 20160, WEEKS",
    "5 week, 50400, WEEKS",
    "14days, 20160, DAYS"
  })
  void givenSupportedUnit_whenCreate_thenTtlAndUnitParsed(
      String argument, long expectedMinutes, ChronoUnit expectedUnit) {
    var hint = (TtlHint) ttlFactory.create(hint("ttl", argument));

    assertThat(hint.getTtl()).contains(Duration.ofMinutes(expectedMinutes));
    assertThat(hint.getTtlUnit()).contains(expectedUnit);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"10 s", "10 seconds", "500 ms", "3 months", "1 year", "fortnight", "14", "days"})
  void givenUnsupportedUnitOrFormat_whenCreate_thenThrows(String argument) {
    assertThatThrownBy(() -> ttlFactory.create(hint("ttl", argument)))
        .isInstanceOf(StatementParserException.class)
        .hasMessageContaining("unit between minute and week");
  }

  @Test
  void givenTwoArgs_whenCreate_thenThrows() {
    assertThatThrownBy(() -> ttlFactory.create(hint("ttl", "14 days", "1 day")))
        .isInstanceOf(StatementParserException.class)
        .hasMessageContaining("one duration argument");
  }

  @Test
  void givenSingleArg_whenCreateCacheHint_thenDurationSet() {
    var hint = (CacheHint) cacheFactory.create(hint("cache", "5 min"));

    assertThat(hint.getDuration()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void givenTwoArgs_whenCreateCacheHint_thenThrows() {
    assertThatThrownBy(() -> cacheFactory.create(hint("cache", "5 min", "1 day")))
        .isInstanceOf(StatementParserException.class);
  }

  @Test
  void givenNullOptions_whenCreate_thenEmptyTtlAndUnit() {
    var hint = (TtlHint) ttlFactory.create(nullArgsHint("ttl", (List<String>) null));

    assertThat(hint.getTtl()).isEmpty();
    assertThat(hint.getTtlUnit()).isEmpty();
  }

  @Test
  void givenNullOptions_whenCreateCacheHint_thenZeroDuration() {
    var hint = (CacheHint) cacheFactory.create(nullArgsHint("cache", (List<String>) null));

    assertThat(hint.getDuration()).isEqualTo(Duration.ZERO);
  }

  @Test
  void givenNullFirstArg_whenCreate_thenThrows() {
    assertThatThrownBy(() -> ttlFactory.create(nullArgsHint("ttl", Arrays.asList((String) null))))
        .isInstanceOf(StatementParserException.class);
  }

  @Test
  void givenNullFirstArg_whenCreateCacheHint_thenThrows() {
    assertThatThrownBy(
            () -> cacheFactory.create(nullArgsHint("cache", Arrays.asList((String) null))))
        .isInstanceOf(StatementParserException.class);
  }

  private static ParsedObject<SqrlHint> nullArgsHint(String name, List<String> args) {
    return new ParsedObject<>(new SqrlHint(name, args), FileLocation.START);
  }
}
