package com.datasqrl.json;

import java.util.Set;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.POJONode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ValueNode;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * Parses a JSON object from string
 */
public class ToJson extends ScalarFunction {

  public static final ObjectMapper mapper = new ObjectMapper();

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
    if (json instanceof FlinkJsonType) {
      return (FlinkJsonType)json;
    }

    return new FlinkJsonType(unboxFlinkToJsonNode(json));
  }

  JsonNode unboxFlinkToJsonNode(Object json) {
    if (json instanceof Row) {
      Row row = (Row) json;
      ObjectNode objectNode = mapper.createObjectNode();
      Set<String> fieldNames = row.getFieldNames(true);
      if (fieldNames == null)
        return mapper.createObjectNode();
      fieldNames.forEach(f -> objectNode.putPOJO(f, unboxFlinkToJsonNode(row.getField(f))));
    } else if (json instanceof Row[]) {
      Row[] rows = (Row[]) json;
      ArrayNode arrayNode = mapper.createArrayNode();
      for (Row row : rows) {
        if (row == null) {
          arrayNode.addNull();
        } else {
          ObjectNode objectNode = mapper.createObjectNode();
          Set<String> fieldNames = row.getFieldNames(true);
          if (fieldNames == null) continue;
          fieldNames.forEach(f -> objectNode.putPOJO(f, unboxFlinkToJsonNode(row.getField(f))));
          arrayNode.add(objectNode);
        }
      }
      return arrayNode;
    }
    return mapper.getNodeFactory().pojoNode(json);
  }
}