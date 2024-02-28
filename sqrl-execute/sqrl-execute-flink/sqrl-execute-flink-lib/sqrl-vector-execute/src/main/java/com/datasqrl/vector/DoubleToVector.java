package com.datasqrl.vector;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a double array to a vector
 */
public class DoubleToVector extends ScalarFunction {

  public FlinkVectorType eval(double[] array) {
    return new FlinkVectorType(array);
  }
}
