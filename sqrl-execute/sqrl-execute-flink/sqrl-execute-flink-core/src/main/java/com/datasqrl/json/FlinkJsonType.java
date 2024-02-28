package com.datasqrl.json;

import org.apache.flink.table.annotation.DataTypeHint;

@DataTypeHint(value = "RAW", bridgedTo = FlinkJsonType.class)
public class FlinkJsonType {
  public String json;

  public FlinkJsonType(String json) {
    this.json = json;
  }

  public String getJson() {
    return json;
  }
}
