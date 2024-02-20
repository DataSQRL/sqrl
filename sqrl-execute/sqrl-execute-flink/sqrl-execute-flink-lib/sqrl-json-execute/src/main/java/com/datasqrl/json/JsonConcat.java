package com.datasqrl.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Merges two JSON objects into one. If two objects share the same key, the value from the later object is used.
 */
public class JsonConcat extends ScalarFunction {

  private final ObjectMapper mapper = new ObjectMapper();

  public FlinkJsonType eval(FlinkJsonType json1, FlinkJsonType json2) {
    if (json1 == null || json2 == null) {
      return null;
    }
    try {
      ObjectNode node1 = (ObjectNode) mapper.readTree(json1.getJson());
      ObjectNode node2 = (ObjectNode) mapper.readTree(json2.getJson());

      node1.setAll(node2);
      return new FlinkJsonType(node1.toString());
    } catch (Exception e) {
      return null;
    }
  }

}