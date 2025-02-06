package com.datasqrl.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.jackson.JacksonMapperFactory;

/**
 * Parses a JSON object from string
 */
public class ToJson extends ScalarFunction {

  public static final ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();

  public FlinkJsonType eval(String json) {
    if (json == null) {
      return null;
    }
    try {
      return new FlinkJsonType(mapper.readTree(json));
    } catch (JsonProcessingException e) {
      return null;
    }
  }


  public FlinkJsonType eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object json) {
    if (json == null) {
      return null;
    }
    if (json instanceof FlinkJsonType type) {
      return type;
    }

    return new FlinkJsonType(unboxFlinkToJsonNode(json));
  }

  JsonNode unboxFlinkToJsonNode(Object json) {
    if (json instanceof Row row) {
      var objectNode = mapper.createObjectNode();
      var fieldNames = row.getFieldNames(true).toArray(new String[0]);  // Get field names in an array
      for (String fieldName : fieldNames) {
        var field = row.getField(fieldName);
        objectNode.set(fieldName, unboxFlinkToJsonNode(field));  // Recursively unbox each field
      }
      return objectNode;
    } else if (json instanceof Row[] rows) {
      var arrayNode = mapper.createArrayNode();
      for (Row row : rows) {
        if (row == null) {
          arrayNode.addNull();
        } else {
          arrayNode.add(unboxFlinkToJsonNode(row));  // Recursively unbox each row in the array
        }
      }
      return arrayNode;
    }
    return mapper.valueToTree(json);  // Directly serialize other types
  }
}