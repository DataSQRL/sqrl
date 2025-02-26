package com.datasqrl.json;

import java.util.Map;
import lombok.Getter;
import lombok.Value;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.annotation.DataTypeHint;

@Value
public class ObjectAgg {

  @DataTypeHint(value = "RAW")
  @Getter
  Map<String, JsonNode> objects;

  public void add(String key, JsonNode value) {
    if (key != null) {
      objects.put(key, value);
    }
  }

  public void remove(String key) {
    if (key != null) {
      objects.remove(key);
    }
  }
}
