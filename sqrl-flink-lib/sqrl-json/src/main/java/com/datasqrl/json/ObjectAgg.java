package com.datasqrl.json;

import java.util.Map;
import lombok.Value;
import org.apache.flink.table.annotation.DataTypeHint;

@Value
public class ObjectAgg {

  @DataTypeHint(value = "RAW")
  Map<String, Object> objects;

  public void add(String key, Object value) {
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