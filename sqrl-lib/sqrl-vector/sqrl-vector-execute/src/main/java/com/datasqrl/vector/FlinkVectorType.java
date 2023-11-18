package com.datasqrl.vector;

import lombok.Value;
import org.apache.flink.table.annotation.DataTypeHint;

@Value
@DataTypeHint(value = "RAW", bridgedTo = FlinkVectorType.class)
public class FlinkVectorType {
  public double[] value;
}
