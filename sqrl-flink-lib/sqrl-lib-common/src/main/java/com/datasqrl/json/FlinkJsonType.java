package com.datasqrl.json;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.annotation.DataTypeHint;

@DataTypeHint(value = "RAW", bridgedTo = FlinkJsonType.class,
    rawSerializer = FlinkJsonTypeSerializer.class)
public class FlinkJsonType {
  public JsonNode json;

  public FlinkJsonType(JsonNode json) {
    this.json = json;
  }

  public JsonNode getJson() {
    return json;
  }
}
