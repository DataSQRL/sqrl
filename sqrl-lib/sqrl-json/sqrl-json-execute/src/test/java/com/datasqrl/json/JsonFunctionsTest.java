package com.datasqrl.json;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;


class JsonFunctionsTest {
  @Nested
  class ToJsonTest {

    @Test
    void testValidJson() {
      String json = "{\"key\":\"value\"}";
      FlinkJsonType result = JsonFunctions.TO_JSON.eval(json);
      assertNotNull(result);
      assertEquals(json, result.getJson());
    }

    @Test
    void testInvalidJson() {
      String json = "Not a JSON";
      FlinkJsonType result = JsonFunctions.TO_JSON.eval(json);
      assertNull(result);
    }
  }

  @Nested
  class JsonToStringTest {

    @Test
    void testNonNullJson() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_TO_STRING.eval(json);
      assertEquals("{\"key\": \"value\"}", result);
    }

    @Test
    void testNullJson() {
      String result = JsonFunctions.JSON_TO_STRING.eval(null);
      assertNull(result);
    }
  }

  @Nested
  class JsonObjectTest {

    @Test
    void testValidKeyValuePairs() {
      FlinkJsonType result = JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2", "value2");
      assertNotNull(result);
      assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", result.getJson());
    }

    @Test
    void testInvalidNumberOfArguments() {
      assertThrows(IllegalArgumentException.class,
          () -> JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2"));
    }
  }
  @Nested
  class JsonArrayTest {

    @Test
    void testArrayWithJsonObjects() {
      FlinkJsonType json1 = new FlinkJsonType("{\"key1\": \"value1\"}");
      FlinkJsonType json2 = new FlinkJsonType("{\"key2\": \"value2\"}");
      FlinkJsonType result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertNotNull(result);
      assertEquals("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]", result.getJson());
    }

    @Test
    void testArrayWithMixedTypes() {
      FlinkJsonType result = JsonFunctions.JSON_ARRAY.eval("stringValue", 123, true);
      assertNotNull(result);
      assertEquals("[\"stringValue\",123,true]", result.getJson());
    }
  }

  @Nested
  class JsonExtractTest {

    @Test
    void testValidPath() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key");
      assertEquals("value", result);
    }

    // Testing eval method with a default value for String
    @Test
    void testStringPathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      String defaultValue = "default";
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertEquals(defaultValue, result);
    }

    // Testing eval method with a default value for boolean
    @Test
    void testBooleanPathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": true}");
      boolean defaultValue = false;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertFalse(result);
    }

    // Testing eval method with a default value for Double
    @Test
    void testDoublePathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": 1.23}");
      Double defaultValue = 4.56;
      Double result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertEquals(defaultValue, result);
    }

    // Testing eval method with a default value for Integer
    @Test
    void testIntegerPathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": 123}");
      Integer defaultValue = 456;
      Integer result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertEquals(defaultValue, result);
    }
    @Test
    void testInvalidPath() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey");
      assertNull(result);
    }
  }

  @Nested
  class JsonQueryTest {

    @Test
    void testValidQuery() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.key");
      assertEquals("\"value\"", result); // Note the JSON representation of a string value
    }

    // Test for a more complex JSON path query
    @Test
    void testComplexQuery() {
      FlinkJsonType json = new FlinkJsonType("{\"key1\": {\"key2\": \"value\"}}");
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.key1.key2");
      assertEquals("\"value\"", result); // JSON representation of the result
    }

    // Test for an invalid query
    @Test
    void testInvalidQuery() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.invalidKey");
      assertNull(result);
    }
  }

  @Nested
  class JsonExistsTest {

    @Test
    void testPathExists() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key");
      assertTrue(result);
    }

    // Test for a path that exists
    @Test
    void testPathExistsComplex() {
      FlinkJsonType json = new FlinkJsonType("{\"key1\": {\"key2\": \"value\"}}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.key2");
      assertTrue(result);
    }

    @Test
    void testPathDoesNotExistComplex() {
      FlinkJsonType json = new FlinkJsonType("{\"key1\": {\"key2\": \"value\"}}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.nonexistentKey");
      assertFalse(result);
    }
    @Test
    void testPathDoesNotExist() {
      FlinkJsonType json = new FlinkJsonType("{\"key\": \"value\"}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.nonexistentKey");
      assertFalse(result);
    }
  }

  @Nested
  class JsonArrayAggTest {

    @Test
    void testAggregateJsonTypes() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, new FlinkJsonType("{\"key1\": \"value1\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, new FlinkJsonType("{\"key2\": \"value2\"}"));

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]", result.getJson());
    }

    @Test
    void testAggregateMixedTypes() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, "stringValue");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, 123);

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[\"stringValue\",123]", result.getJson());
    }
  }

  @Nested
  class JsonObjectAggTest {

    @Test
    void testAggregateJsonTypes() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", new FlinkJsonType("{\"nestedKey1\": \"nestedValue1\"}"));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", new FlinkJsonType("{\"nestedKey2\": \"nestedValue2\"}"));

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key1\":{\"nestedKey1\":\"nestedValue1\"},\"key2\":{\"nestedKey2\":\"nestedValue2\"}}", result.getJson());
    }

    @Test
    void testAggregateWithOverwritingKeys() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value1");
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value2");

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key\":\"value2\"}", result.getJson()); // The last value for the same key should be retained
    }
  }
}