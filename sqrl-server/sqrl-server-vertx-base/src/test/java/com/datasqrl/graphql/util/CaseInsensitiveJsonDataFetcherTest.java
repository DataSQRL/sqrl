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

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class CaseInsensitiveJsonDataFetcherTest {

  private final CaseInsensitiveJsonDataFetcher fetcher =
      new CaseInsensitiveJsonDataFetcher("testkey");

  @Test
  void caseInsensitivePropertyFetcherNonNullValue() {
    var jsonObject = new JsonObject();
    jsonObject.put("TestKey", "TestValue");

    assertThat(fetcher.fetchJsonObject(jsonObject)).isEqualTo("TestValue");
  }

  @Test
  void caseInsensitivePropertyFetcherNullValue() {
    // Creating a JsonObject with a key but null value
    var jsonObject = new JsonObject();
    jsonObject.put("TestKey", null);

    assertThat(fetcher.fetchJsonObject(jsonObject)).isNull();
  }

  @Test
  void caseInsensitivePropertyFetcherNoMatch() {
    // Creating a JsonObject without the matching key
    var jsonObject = new JsonObject();
    jsonObject.put("AnotherKey", "SomeValue");

    assertThat(fetcher.fetchJsonObject(jsonObject)).isNull();
  }
}
