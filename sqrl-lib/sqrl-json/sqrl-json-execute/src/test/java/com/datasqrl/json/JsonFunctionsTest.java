package com.datasqrl.json;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;


class JsonFunctionsTest {

  ObjectMapper mapper = new ObjectMapper();
  @SneakyThrows
  public JsonNode readTree(String content) {
    return mapper.readTree(content);
  }

  @Nested
  class ToJsonTest {

    @Test
    void testValidJson() {
      String json = "{\"key\":\"value\"}";
      JsonNode result = JsonFunctions.TO_JSON.eval(json);
      assertNotNull(result);
      assertEquals(json, result);
    }

    @Test
    void testInvalidJson() {
      String json = "Not a JSON";
      JsonNode result = JsonFunctions.TO_JSON.eval(json);
      assertNull(result);
    }

    @Test
    void testNullInput() {
      assertNull(JsonFunctions.TO_JSON.eval(null));
    }
  }

  @Nested
  class JsonToStringTest {

    @Test
    void testNonNullJson() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_TO_STRING.eval(json);
      assertEquals(readTree("{\"key\": \"value\"}"), result);
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
      JsonNode result = JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2", "value2");
      assertNotNull(result);
      assertEquals(readTree("{\"key1\":\"value1\",\"key2\":\"value2\"}"), result);
    }

    @Test
    void testInvalidNumberOfArguments() {
      assertThrows(IllegalArgumentException.class,
          () -> JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2"));
    }

    @Test
    void testNullKeyOrValue() {
      JsonNode resultWithNullValue = JsonFunctions.JSON_OBJECT.eval("key1", null);
      assertNotNull(resultWithNullValue);
      assertEquals(readTree("{\"key1\":null}"), resultWithNullValue);
    }
  }
  @Nested
  class JsonArrayTest {

    @Test
    void testArrayWithJsonObjects() {
      JsonNode json1 = readTree("{\"key1\": \"value1\"}");
      JsonNode json2 = readTree("{\"key2\": \"value2\"}");
      JsonNode result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertNotNull(result);
      assertEquals(readTree("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]"), result);
    }

    @Test
    void testArrayWithMixedTypes() {
      JsonNode result = JsonFunctions.JSON_ARRAY.eval("stringValue", 123, true);
      assertNotNull(result);
      assertEquals(readTree("[\"stringValue\",123,true]"), result);
    }

    @Test
    void testArrayWithNullValues() {
      JsonNode result = JsonFunctions.JSON_ARRAY.eval((Object) null);
      assertNotNull(result);
      assertEquals(readTree("[null]"), result);
    }
  }

  @Nested
  class JsonExtractTest {

    @Test
    void testValidPath() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key");
      assertEquals("value", result);
    }

    // Testing eval method with a default value for String
    @Test
    void testStringPathWithDefaultValue() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      String defaultValue = "default";
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertEquals(defaultValue, result);
    }

    // Testing eval method with a default value for boolean
    @Test
    void testBooleanPathWithDefaultValue() {
      JsonNode json = readTree("{\"key\": true}");
      boolean defaultValue = false;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertFalse(result);
    }

    // Testing eval method with a default value for Double
    @Test
    void testDoublePathWithDefaultValue() {
      JsonNode json = readTree("{\"key\": 1.23}");
      Double defaultValue = 4.56;
      Double result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      Assertions.assertEquals(defaultValue, result);
    }

    // Testing eval method with a default value for Integer
    @Test
    void testIntegerPathWithDefaultValue() {
      JsonNode json = readTree("{\"key\": 123}");
      Integer defaultValue = 456;
      Integer result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      Assertions.assertEquals(defaultValue, result);
    }
    @Test
    void testInvalidPath() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey");
      assertNull(result);
    }
  }

  @Nested
  class JsonQueryTest {

    @Test
    void testValidQuery() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.key");
      Assertions.assertEquals("\"value\"", result); // Note the JSON representation of a string value
    }

    // Test for a more complex JSON path query
    @Test
    void testComplexQuery() {
      JsonNode json = readTree("{\"key1\": {\"key2\": \"value\"}}");
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.key1.key2");
      assertEquals("\"value\"", result); // JSON representation of the result
    }

    // Test for an invalid query
    @Test
    void testInvalidQuery() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.invalidKey");
      assertNull(result);
    }
  }

  @Nested
  class JsonExistsTest {

    @Test
    void testPathExists() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key");
      assertTrue(result);
    }

    // Test for a path that exists
    @Test
    void testPathExistsComplex() {
      JsonNode json = readTree("{\"key1\": {\"key2\": \"value\"}}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.key2");
      assertTrue(result);
    }

    @Test
    void testPathDoesNotExistComplex() {
      JsonNode json = readTree("{\"key1\": {\"key2\": \"value\"}}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.nonexistentKey");
      assertFalse(result);
    }
    @Test
    void testPathDoesNotExist() {
      JsonNode json = readTree("{\"key\": \"value\"}");
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.nonexistentKey");
      assertFalse(result);
    }

    @Test
    void testNullInput() {
      Boolean result = JsonFunctions.JSON_EXISTS.eval(null, "$.key");
      assertNull(result);
    }
  }

  @Nested
  class JsonConcatTest {

    @Test
    void testSimpleMerge() {
      JsonNode json1 = readTree("{\"key1\": \"value1\"}");
      JsonNode json2 = readTree("{\"key2\": \"value2\"}");
      JsonNode result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
      assertEquals(readTree("{\"key1\":\"value1\",\"key2\":\"value2\"}"), result);
    }

    @Test
    void testOverlappingKeys() {
      JsonNode json1 = readTree("{\"key\": \"value1\"}");
      JsonNode json2 = readTree("{\"key\": \"value2\"}");
      JsonNode result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
      assertEquals(readTree("{\"key\":\"value2\"}"), result);
    }

    @Test
    void testNullInput() {
      JsonNode json1 = readTree("{\"key1\": \"value1\"}");
      JsonNode result = JsonFunctions.JSON_CONCAT.eval(json1, null);
      assertNull(result);
    }

    @Test
    void testNullInput2() {
      JsonNode json1 = readTree("{\"key1\": \"value1\"}");
      JsonNode result = JsonFunctions.JSON_CONCAT.eval(null, json1);
      assertNull(result);
    }
//
//    @Test
//    void testInvalidJson() {
//      JsonNode json1 = readTree("{\"key1\": \"value1\"}");
//      JsonNode json2 = readTree("{\"key2\": \"value2\"}");
//      JsonNode result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
//      assertNull(result);
//    }
  }
  @Nested
  class JsonArrayAggTest {

    @Test
    void testAggregateJsonTypes() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, (ObjectNode) readTree("{\"key1\": \"value1\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, (ObjectNode)readTree("{\"key2\": \"value2\"}"));

      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]"), result);
    }

    @Test
    void testAggregateMixedTypes() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, "stringValue");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, 123);

      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("[\"stringValue\",123]"), result);
    }

    @Test
    void testAccumulateNullValues() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator,(ObjectNode)null);
      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertEquals(readTree("[null]"), result);
    }

    @Test
    void testArrayWithNullElements() {
      JsonNode json1 = readTree("{\"key1\": \"value1\"}");
      JsonNode json2 = null; // null JSON object
      JsonNode result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertNotNull(result);
      // Depending on implementation, the result might include the null or ignore it
      assertEquals(readTree("[{\"key1\":\"value1\"},null]"), result);
    }

    @Test
    void testRetractIntTypes() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, 1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, 2);

      // Now retract one of the JSON objects
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, 1);

      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("[2]"), result);
    }

    @Test
    void testRetractJsonTypes() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonNode json1 = readTree("{\"key\": \"value1\"}");
      JsonNode json2 = readTree("{\"key\": \"value2\"}");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json2);

      // Now retract one of the JSON objects
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, json1);

      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("[{\"key\":\"value2\"}]"), result);
    }

    @Test
    void testRetractNullJsonType() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonNode json1 = readTree("{\"key\": \"value1\"}");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, (ObjectNode)json1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator,(ObjectNode) null);

      // Now retract a null JSON object
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, (JsonNode) null);

      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("[{\"key\":\"value1\"}]"), result);
    }

    @Test
    void testRetractNullFromNonExisting() {
      JsonFunctions.ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonNode json1 = readTree("{\"key\": \"value1\"}");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator,(ObjectNode) json1);

      // Attempt to retract a null value that was never accumulated
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator,(JsonNode) null);

      JsonNode result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("[{\"key\":\"value1\"}]"), result);
    }
  }

  @Nested
  class JsonObjectAggTest {

    @Test
    void testAggregateJsonTypes() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", readTree("{\"nestedKey1\": \"nestedValue1\"}"));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", readTree("{\"nestedKey2\": \"nestedValue2\"}"));

      JsonNode result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("{\"key1\":{\"nestedKey1\":\"nestedValue1\"},\"key2\":{\"nestedKey2\":\"nestedValue2\"}}"), result);
    }

    @Test
    void testAggregateWithOverwritingKeys() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value1");
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value2");

      JsonNode result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("{\"key\":\"value2\"}"), result); // The last value for the same key should be retained
    }

    @Test
    void testNullKey() {
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.JSON_OBJECT.eval(null, "value1"));
    }

    @Test
    void testNullValue() {
      JsonNode result = JsonFunctions.JSON_OBJECT.eval("key1", null);
      assertNotNull(result);
      assertEquals(readTree("{\"key1\":null}"), result);
    }

    @Test
    void testNullKeyValue() {
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.JSON_OBJECT.eval(null, null));
    }

    @Test
    void testArrayOfNullValues() {
      JsonNode result = JsonFunctions.JSON_OBJECT.eval("key1", new Object[]{null, null, null});
      assertNotNull(result);
      // The expected output might vary based on how the function is designed to handle this case
      assertEquals(readTree("{\"key1\":[null,null,null]}"), result);
    }

    @Test
    void testRetractJsonTypes() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", readTree("{\"nestedKey1\": \"nestedValue1\"}"));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", readTree("{\"nestedKey2\": \"nestedValue2\"}"));

      // Now retract a key-value pair
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, "key1", readTree("{\"nestedKey1\": \"nestedValue1\"}"));

      JsonNode result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("{\"key2\":{\"nestedKey2\":\"nestedValue2\"}}"), result);
    }

    @Test
    void testRetractNullJsonValue() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", readTree("{\"nestedKey1\": \"nestedValue1\"}"));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", (JsonNode) null);

      // Now retract a null value
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, "key2", (JsonNode) null);

      JsonNode result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("{\"key1\":{\"nestedKey1\":\"nestedValue1\"}}"), result);
    }

    @Test
    void testRetractNullKey() {
      JsonFunctions.ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", readTree("{\"nestedKey1\": \"nestedValue1\"}"));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, null, "someValue");

      // Attempt to retract a key-value pair where the key is null
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, null, "someValue");

      JsonNode result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals(readTree("{\"key1\":{\"nestedKey1\":\"nestedValue1\"}}"), result);
    }
  }

  public void assertEquals(JsonNode a, JsonNode b) {
    Assertions.assertEquals(a.toString(), b.toString());
  }

  public void assertEquals(String a, JsonNode b) {
    Assertions.assertEquals(a, b.toString());
  }

  public void assertEquals(JsonNode a, String b) {
    Assertions.assertEquals(a.toString(), b);
  }

  public void assertEquals(String a, String b) {
    Assertions.assertEquals(a, b);
  }
}