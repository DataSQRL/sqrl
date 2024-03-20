package com.datasqrl.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.ScalarFunction;

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
      return new FlinkJsonType(mapper.readTree(json).toString());
    } catch (JsonProcessingException e) {
      return null;
    }
  }
}