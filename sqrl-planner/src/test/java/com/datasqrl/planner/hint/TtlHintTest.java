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
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class TtlHintTest {

  private final TtlHint.TtlHintFactory ttlFactory = new TtlHint.TtlHintFactory();
  private final CacheHint.CacheHintFactory cacheFactory = new CacheHint.CacheHintFactory();

  private static ParsedObject<SqrlHint> hint(String name, String... args) {
    return new ParsedObject<>(new SqrlHint(name, List.of(args)), FileLocation.START);
  }

  @Test
  void givenSingleDurationArg_whenCreate_thenTtlSetAndNoPartitionInterval() {
    var hint = (TtlHint) ttlFactory.create(hint("ttl", "14 days"));

    assertThat(hint.getTtl()).contains(Duration.ofDays(14));
    assertThat(hint.getPartitionInterval()).isEmpty();
  }

  @Test
  void givenNoArgs_whenCreate_thenEmptyTtlAndPartitionInterval() {
    var hint = (TtlHint) ttlFactory.create(hint("ttl"));

    assertThat(hint.getTtl()).isEmpty();
    assertThat(hint.getPartitionInterval()).isEmpty();
  }

  @Test
  void givenDurationAndPartitionIntervalArgs_whenCreate_thenBothSet() {
    var hint = (TtlHint) ttlFactory.create(hint("ttl", "14 days", "1 day"));

    assertThat(hint.getTtl()).contains(Duration.ofDays(14));
    assertThat(hint.getPartitionInterval()).contains("1 day");
  }

  @Test
  void givenPluralPartitionInterval_whenCreate_thenAccepted() {
    var hint = (TtlHint) ttlFactory.create(hint("ttl", "90 days", "2 weeks"));

    assertThat(hint.getPartitionInterval()).contains("2 weeks");
  }

  @Test
  void givenInvalidPartitionInterval_whenCreate_thenThrows() {
    assertThatThrownBy(() -> ttlFactory.create(hint("ttl", "14 days", "daily")))
        .isInstanceOf(StatementParserException.class)
        .hasMessageContaining("partition interval");
  }

  @Test
  void givenThreeArgs_whenCreate_thenThrows() {
    assertThatThrownBy(() -> ttlFactory.create(hint("ttl", "14 days", "1 day", "extra")))
        .isInstanceOf(StatementParserException.class);
  }

  @Test
  void givenInvalidDuration_whenCreate_thenThrows() {
    assertThatThrownBy(() -> ttlFactory.create(hint("ttl", "fortnight")))
        .isInstanceOf(StatementParserException.class)
        .hasMessageContaining("duration");
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
  void givenNullOptions_whenCreate_thenEmptyTtlAndPartitionInterval() {
    var hint = (TtlHint) ttlFactory.create(nullArgsHint("ttl", (List<String>) null));

    assertThat(hint.getTtl()).isEmpty();
    assertThat(hint.getPartitionInterval()).isEmpty();
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

  @Test
  void givenNullPartitionInterval_whenCreate_thenThrows() {
    assertThatThrownBy(() -> ttlFactory.create(nullArgsHint("ttl", Arrays.asList("14 days", null))))
        .isInstanceOf(StatementParserException.class)
        .hasMessageContaining("partition interval");
  }

  private static ParsedObject<SqrlHint> nullArgsHint(String name, List<String> args) {
    return new ParsedObject<>(new SqrlHint(name, args), FileLocation.START);
  }
}
