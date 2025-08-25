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
package com.datasqrl.graphql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class VertxContextTest {

  @Test
  void caseInsensitivePropertyFetcherNonNullValue() {
    var jsonObject = new JsonObject();
    jsonObject.put("TestKey", "TestValue");
    var result = testValue(jsonObject);
    assertThat(result).isEqualTo("TestValue");
  }

  @Test
  void caseInsensitivePropertyFetcherNullValue() {
    // Creating a JsonObject with a key but null value
    var jsonObject = new JsonObject();
    jsonObject.put("TestKey", null);

    var result = testValue(jsonObject);
    assertThat(result).isNull();
  }

  @Test
  void caseInsensitivePropertyFetcherNoMatch() {
    // Creating a JsonObject without the matching key
    var jsonObject = new JsonObject();
    jsonObject.put("AnotherKey", "SomeValue");

    var result = testValue(jsonObject);
    assertThat(result).isNull();
  }

  @SneakyThrows
  Object testValue(JsonObject jsonObject) {
    // Mocking DataFetchingEnvironment
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);

    when(env.getSource()).thenReturn(jsonObject);

    DataFetcher<Object> fetcher =
        VertxContext.VertxCreateCaseInsensitivePropertyDataFetcher.createCaseInsensitive("testkey");

    return fetcher.get(env);
  }
}
