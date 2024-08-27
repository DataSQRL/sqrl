package com.datasqrl.json;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Extracts a value from the JSON object based on the provided JSON path. An optional third argument
 * can be provided to specify a default value when the given JSON path does not yield a value for
 * the JSON object.
 */
public class JsonExtract extends ScalarFunction {

  public String eval(FlinkJsonType input, String pathSpec) {
    if (input == null) {
      return null;
    }
    try {
      JsonNode jsonNode = input.getJson();
      ReadContext ctx = JsonPath.parse(jsonNode.toString());
      Object value = ctx.read(pathSpec);
      if (value == null) {
        return null;
      }
      return value.toString();
    } catch (Exception e) {
      return null;
    }
  }

  public String eval(FlinkJsonType input, String pathSpec, String defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      JsonPath parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, String.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public Boolean eval(FlinkJsonType input, String pathSpec, Boolean defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      JsonPath parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, Boolean.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public Double eval(FlinkJsonType input, String pathSpec, Double defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      JsonPath parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, Double.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }

  public Integer eval(FlinkJsonType input, String pathSpec, Integer defaultValue) {
    if (input == null) {
      return null;
    }
    try {
      ReadContext ctx = JsonPath.parse(input.getJson().toString());
      JsonPath parse = JsonPath.compile(pathSpec);
      return ctx.read(parse, Integer.class);
    } catch (Exception e) {
      return defaultValue;
    }
  }
}
