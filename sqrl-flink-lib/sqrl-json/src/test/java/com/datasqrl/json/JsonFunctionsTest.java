package com.datasqrl.json;

import static org.junit.jupiter.api.Assertions.*;

import lombok.SneakyThrows;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;


class JsonFunctionsTest {
  ObjectMapper mapper = new ObjectMapper();
  
  @SneakyThrows
  JsonNode readTree(String val) {
    return mapper.readTree(val);
  }
  
  @Nested
  class ToJsonTest {

    @Test
    void testUnicodeJson() {
      Row row = Row.withNames();
      row.setField("key", "”value”");
      Row[] rows = new Row[]{row};
      FlinkJsonType result = JsonFunctions.TO_JSON.eval(rows);
      assertNotNull(result);
      assertEquals("[{\"key\":\"”value”\"}]", result.getJson().toString());
    }

    @Test
    void testValidJson() {
      String json = "{\"key\":\"value\"}";
      FlinkJsonType result = JsonFunctions.TO_JSON.eval(json);
      assertNotNull(result);
      assertEquals(json, result.getJson().toString());
    }

    @Test
    void testInvalidJson() {
      String json = "Not a JSON";
      FlinkJsonType result = JsonFunctions.TO_JSON.eval(json);
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
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      String result = JsonFunctions.JSON_TO_STRING.eval(json);
      assertEquals("{\"key\":\"value\"}", result);
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
      assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", result.getJson().toString());
    }

    @Test
    void testInvalidNumberOfArguments() {
      assertThrows(IllegalArgumentException.class,
          () -> JsonFunctions.JSON_OBJECT.eval("key1", "value1", "key2"));
    }

    @Test
    void testNullKeyOrValue() {
      FlinkJsonType resultWithNullValue = JsonFunctions.JSON_OBJECT.eval("key1", null);
      assertNotNull(resultWithNullValue);
      assertEquals("{\"key1\":null}", resultWithNullValue.getJson().toString());
    }
  }
  @Nested
  class JsonArrayTest {

    @Test
    void testArrayWithJsonObjects() {
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      FlinkJsonType json2 = new FlinkJsonType(readTree("{\"key2\": \"value2\"}"));
      FlinkJsonType result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertNotNull(result);
      assertEquals("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]", result.getJson().toString());
    }

    @Test
    void testArrayWithMixedTypes() {
      FlinkJsonType result = JsonFunctions.JSON_ARRAY.eval("stringValue", 123, true);
      assertNotNull(result);
      assertEquals("[\"stringValue\",123,true]", result.getJson().toString());
    }

    @Test
    void testArrayWithNullValues() {
      FlinkJsonType result = JsonFunctions.JSON_ARRAY.eval((Object) null);
      assertNotNull(result);
      assertEquals("[null]", result.getJson().toString());
    }
  }

  @Nested
  class JsonExtractTest {

    @Test
    void testValidPath() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key");
      assertEquals("value", result);
    }

    @Test
    void testValidPathBoolean() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": true}"));
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key");
      assertEquals("true", result);
    }

    // Testing eval method with a default value for String
    @Test
    void testStringPathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      String defaultValue = "default";
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertEquals(defaultValue, result);
    }

    // Testing eval method with a default value for boolean
    @Test
    void testBooleanPathNormalWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": true}"));
      boolean defaultValue = false;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key", defaultValue);
      assertTrue(result);
    }

    @Test
    void testBooleanPathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": true}"));
      boolean defaultValue = false;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertFalse(result);
    }

    // Testing eval method with a default value for boolean:false
    @Test
    void testBooleanPathWithDefaultValueTrue() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": true}"));
      boolean defaultValue = true;
      boolean result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey", defaultValue);
      assertTrue(result);
    }

    // Testing eval method with a default value for Double
    @Test
    void testDoublePathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": 1.23}"));
      Double defaultValue = 4.56;
      Double result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key", defaultValue);
      assertEquals(1.23, result);
    }

    // Testing eval method with a default value for Integer
    @Test
    void testIntegerPathWithDefaultValue() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": 123}"));
      Integer defaultValue = 456;
      Integer result = JsonFunctions.JSON_EXTRACT.eval(json, "$.key", defaultValue);
      assertEquals(123, result);
    }

    @Test
    void testInvalidPath() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      String result = JsonFunctions.JSON_EXTRACT.eval(json, "$.nonexistentKey");
      assertNull(result);
    }
  }

  @Nested
  class JsonQueryTest {

    @Test
    void testValidQuery() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.key");
      assertEquals("\"value\"", result); // Note the JSON representation of a string value
    }

    // Test for a more complex JSON path query
    @Test
    void testComplexQuery() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key1\": {\"key2\": \"value\"}}"));
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.key1.key2");
      assertEquals("\"value\"", result); // JSON representation of the result
    }

    // Test for an invalid query
    @Test
    void testInvalidQuery() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      String result = JsonFunctions.JSON_QUERY.eval(json, "$.invalidKey");
      assertNull(result);
    }
  }

  @Nested
  class JsonExistsTest {

    @Test
    void testPathExists() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key");
      assertTrue(result);
    }

    // Test for a path that exists
    @Test
    void testPathExistsComplex() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key1\": {\"key2\": \"value\"}}"));
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.key2");
      assertTrue(result);
    }

    @Test
    void testPathDoesNotExistComplex() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key1\": {\"key2\": \"value\"}}"));
      Boolean result = JsonFunctions.JSON_EXISTS.eval(json, "$.key1.nonexistentKey");
      assertFalse(result);
    }
    @Test
    void testPathDoesNotExist() {
      FlinkJsonType json = new FlinkJsonType(readTree("{\"key\": \"value\"}"));
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
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      FlinkJsonType json2 = new FlinkJsonType(readTree("{\"key2\": \"value2\"}"));
      FlinkJsonType result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
      assertEquals("{\"key1\":\"value1\",\"key2\":\"value2\"}", result.getJson().toString());
    }

    @Test
    void testOverlappingKeys() {
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      FlinkJsonType json2 = new FlinkJsonType(readTree("{\"key\": \"value2\"}"));
      FlinkJsonType result = JsonFunctions.JSON_CONCAT.eval(json1, json2);
      assertEquals("{\"key\":\"value2\"}", result.getJson().toString());
    }

    @Test
    void testNullInput() {
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      FlinkJsonType result = JsonFunctions.JSON_CONCAT.eval(json1, null);
      assertNull(result);
    }

    @Test
    void testNullInput2() {
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      FlinkJsonType result = JsonFunctions.JSON_CONCAT.eval(null, json1);
      assertNull(result);
    }
  }
  @Nested
  class JsonArrayAggTest {

    @Test
    void testAggregateJsonTypes() {
      ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, new FlinkJsonType(readTree("{\"key1\": \"value1\"}")));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, new FlinkJsonType(readTree("{\"key2\": \"value2\"}")));

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]", result.getJson().toString());
    }

    @Test
    void testAggregateMixedTypes() {
      ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, "stringValue");
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, 123);

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[\"stringValue\",123]", result.getJson().toString());
    }

    @Test
    void testAccumulateNullValues() {
      ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, (FlinkJsonType) null);
      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertEquals("[null]", result.getJson().toString());
    }

    @Test
    void testArrayWithNullElements() {
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key1\": \"value1\"}"));
      FlinkJsonType json2 = null; // null JSON object
      FlinkJsonType result = JsonFunctions.JSON_ARRAY.eval(json1, json2);
      assertNotNull(result);
      // Depending on implementation, the result might include the null or ignore it
      assertEquals("[{\"key1\":\"value1\"},null]", result.getJson().toString());
    }
    @Test
    void testRetractJsonTypes() {
      ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      FlinkJsonType json2 = new FlinkJsonType(readTree("{\"key\": \"value2\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json2);

      // Now retract one of the JSON objects
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, json1);

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[{\"key\":\"value2\"}]", result.getJson().toString());
    }

    @Test
    void testRetractNullJsonType() {
      ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator,(FlinkJsonType) null);

      // Now retract a null JSON object
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator, (FlinkJsonType) null);

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[{\"key\":\"value1\"}]", result.getJson().toString());
    }

    @Test
    void testRetractNullFromNonExisting() {
      ArrayAgg accumulator = JsonFunctions.JSON_ARRAYAGG.createAccumulator();
      FlinkJsonType json1 = new FlinkJsonType(readTree("{\"key\": \"value1\"}"));
      JsonFunctions.JSON_ARRAYAGG.accumulate(accumulator, json1);

      // Attempt to retract a null value that was never accumulated
      JsonFunctions.JSON_ARRAYAGG.retract(accumulator,(FlinkJsonType) null);

      FlinkJsonType result = JsonFunctions.JSON_ARRAYAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("[{\"key\":\"value1\"}]", result.getJson().toString());
    }
  }

  @Nested
  class JsonObjectAggTest {

    @Test
    void testAggregateJsonTypes() {
      ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", new FlinkJsonType(readTree("{\"nestedKey2\": \"nestedValue2\"}")));

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key1\":{\"nestedKey1\":\"nestedValue1\"},\"key2\":{\"nestedKey2\":\"nestedValue2\"}}", result.getJson().toString());
    }

    @Test
    void testAggregateWithOverwritingKeys() {
      ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value1");
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key", "value2");

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key\":\"value2\"}", result.getJson().toString()); // The last value for the same key should be retained
    }

    @Test
    void testNullKey() {
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.JSON_OBJECT.eval(null, "value1"));
    }

    @Test
    void testNullValue() {
      FlinkJsonType result = JsonFunctions.JSON_OBJECT.eval("key1", null);
      assertNotNull(result);
      assertEquals("{\"key1\":null}", result.getJson().toString());
    }

    @Test
    void testNullKeyValue() {
      assertThrows(IllegalArgumentException.class, () -> JsonFunctions.JSON_OBJECT.eval(null, null));
    }

    @Test
    void testArrayOfNullValues() {
      FlinkJsonType result = JsonFunctions.JSON_OBJECT.eval("key1", new Object[]{null, null, null});
      assertNotNull(result);
      // The expected output might vary based on how the function is designed to handle this case
      assertEquals("{\"key1\":[null,null,null]}", result.getJson().toString());
    }

    @Test
    void testRetractJsonTypes() {
      ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", new FlinkJsonType(readTree("{\"nestedKey2\": \"nestedValue2\"}")));

      // Now retract a key-value pair
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key2\":{\"nestedKey2\":\"nestedValue2\"}}", result.getJson().toString());
    }

    @Test
    void testRetractNullJsonValue() {
      ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key2", (FlinkJsonType) null);

      // Now retract a null value
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, "key2", (FlinkJsonType) null);

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key1\":{\"nestedKey1\":\"nestedValue1\"}}", result.getJson().toString());
    }

    @Test
    void testRetractNullKey() {
      ObjectAgg accumulator = JsonFunctions.JSON_OBJECTAGG.createAccumulator();
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, "key1", new FlinkJsonType(readTree("{\"nestedKey1\": \"nestedValue1\"}")));
      JsonFunctions.JSON_OBJECTAGG.accumulate(accumulator, null, "someValue");

      // Attempt to retract a key-value pair where the key is null
      JsonFunctions.JSON_OBJECTAGG.retract(accumulator, null, "someValue");

      FlinkJsonType result = JsonFunctions.JSON_OBJECTAGG.getValue(accumulator);
      assertNotNull(result);
      assertEquals("{\"key1\":{\"nestedKey1\":\"nestedValue1\"}}", result.getJson().toString());
    }
  }
}