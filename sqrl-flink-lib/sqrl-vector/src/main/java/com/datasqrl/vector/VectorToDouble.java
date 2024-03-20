package com.datasqrl.vector;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a vector to a double array
 */
public class VectorToDouble extends ScalarFunction {

  public double[] eval(FlinkVectorType vectorType) {
    return vectorType.getValue();
  }

}