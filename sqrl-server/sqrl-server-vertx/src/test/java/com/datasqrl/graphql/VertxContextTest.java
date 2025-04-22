package com.datasqrl.graphql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.json.JsonObject;
import lombok.SneakyThrows;

class VertxContextTest {

  @Test
  void testCaseInsensitivePropertyFetcherNonNullValue() {
    var jsonObject = new JsonObject();
    jsonObject.put("TestKey", "TestValue");
    var result = testValue(jsonObject);
    assertEquals("TestValue", result);
  }

  @Test
  void testCaseInsensitivePropertyFetcherNullValue() {
    // Creating a JsonObject with a key but null value
    var jsonObject = new JsonObject();
    jsonObject.put("TestKey", null);

    var result = testValue(jsonObject);
    assertNull(result);
  }

  @Test
  void testCaseInsensitivePropertyFetcherNoMatch() {
    // Creating a JsonObject without the matching key
    var jsonObject = new JsonObject();
    jsonObject.put("AnotherKey", "SomeValue");

    var result = testValue(jsonObject);
    assertNull(result);
  }

  @SneakyThrows
  Object testValue(JsonObject jsonObject) {
    // Mocking DataFetchingEnvironment
    DataFetchingEnvironment env = Mockito.mock(DataFetchingEnvironment.class);

    when(env.getSource()).thenReturn(jsonObject);

    DataFetcher<Object> fetcher = VertxContext.VertxCreateCaseInsensitivePropertyDataFetcher.createCaseInsensitive(
        "testkey");

    return fetcher.get(env);
  }
}