package com.datasqrl.vector;

import org.apache.flink.table.annotation.DataTypeHint;

@DataTypeHint(value = "RAW", bridgedTo = FlinkVectorType.class, rawSerializer = FlinkVectorTypeSerializer.class)
public class FlinkVectorType {
  public double[] value;

  public FlinkVectorType(double[] value) {
    this.value = value;
  }

  public double[] getValue() {
    return value;
  }
}
