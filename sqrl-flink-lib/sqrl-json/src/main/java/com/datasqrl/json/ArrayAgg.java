package com.datasqrl.json;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.annotation.DataTypeHint;

@Value
@EqualsAndHashCode
public class ArrayAgg {

  @DataTypeHint(value = "RAW")
  private List<JsonNode> objects;

  public void add(JsonNode value) {
    objects.add(value);
  }

  public void remove(JsonNode value) {
    objects.remove(value);
  }
}
