package com.datasqrl.vector;

import org.apache.flink.table.functions.ScalarFunction;

public class DoubleToVector extends ScalarFunction {

  public FlinkVectorType eval(double[] array) {
    return new FlinkVectorType(array);
  }
}
