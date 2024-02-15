package com.datasqrl.json;

import java.util.List;
import lombok.Value;
import org.apache.flink.table.annotation.DataTypeHint;

@Value
public class ArrayAgg {

  @DataTypeHint(value = "RAW")
  private List<Object> objects;

  public void add(Object value) {
    objects.add(value);
  }

  public void remove(Object value) {
    objects.remove(value);
  }
}
