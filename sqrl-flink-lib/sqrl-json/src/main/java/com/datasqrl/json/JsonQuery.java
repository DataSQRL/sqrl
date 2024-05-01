package com.datasqrl.json;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * For a given JSON object, executes a JSON path query against the object and returns the result as string.
 */
public class JsonQuery extends ScalarFunction {

  public String eval(FlinkJsonType input, String pathSpec) {
    if (input == null) {
      return null;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode jsonNode = input.getJson();
      ReadContext ctx = JsonPath.parse(jsonNode.toString());
      Object result = ctx.read(pathSpec);
      return mapper.writeValueAsString(result); // Convert the result back to JSON string
    } catch (Exception e) {
      return null;
    }
  }

}