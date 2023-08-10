package com.datasqrl.calcite.type;

import lombok.Value;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.Arrays;

@Value
public class MyVectorType {
    public @DataTypeHint("ARRAY<DOUBLE>") double[] vector;

    public String input;
    public @DataTypeHint("RAW") Class<?> modelClass;

  @Override
  public String toString() {
    return "<" + Arrays.toString(vector) + ", " + input + ", " + modelClass + ">";
  }

  public double[] toArray() {
    return vector;
  }
}
