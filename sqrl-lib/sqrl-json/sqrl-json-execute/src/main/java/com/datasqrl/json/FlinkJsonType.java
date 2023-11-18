package com.datasqrl.json;

import lombok.Value;
import org.apache.flink.table.annotation.DataTypeHint;

@Value
@DataTypeHint(value = "RAW", bridgedTo = FlinkJsonType.class)
public class FlinkJsonType {
  public String json;
}
