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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CaseInsensitiveJsonDataFetcherTest {

  private final CaseInsensitiveJsonDataFetcher fetcher =
      new CaseInsensitiveJsonDataFetcher("testkey");

  @Test
  void givenMapWithDifferentCaseKey_whenFetchFromMap_thenReturnsValue() {
    Map<String, Object> map = new HashMap<>();
    map.put("TestKey", "TestValue");

    assertThat(fetcher.fetchFromMap(map)).isEqualTo("TestValue");
  }

  @Test
  void givenMapWithExactCaseKey_whenFetchFromMap_thenReturnsValue() {
    Map<String, Object> map = new HashMap<>();
    map.put("testkey", "TestValue");

    assertThat(fetcher.fetchFromMap(map)).isEqualTo("TestValue");
  }

  @Test
  void givenMapWithNullValue_whenFetchFromMap_thenReturnsNull() {
    Map<String, Object> map = new HashMap<>();
    map.put("TestKey", null);

    assertThat(fetcher.fetchFromMap(map)).isNull();
  }

  @Test
  void givenMapWithoutMatchingKey_whenFetchFromMap_thenReturnsNull() {
    Map<String, Object> map = new HashMap<>();
    map.put("AnotherKey", "SomeValue");

    assertThat(fetcher.fetchFromMap(map)).isNull();
  }

  @Test
  void givenMapWithUppercaseKey_whenFetchFromMap_thenReturnsValue() {
    Map<String, Object> map = new HashMap<>();
    map.put("TESTKEY", "TestValue");

    assertThat(fetcher.fetchFromMap(map)).isEqualTo("TestValue");
  }
}
