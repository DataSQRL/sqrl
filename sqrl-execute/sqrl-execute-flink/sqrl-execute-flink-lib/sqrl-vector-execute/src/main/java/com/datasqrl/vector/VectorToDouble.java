package com.datasqrl.vector;

import org.apache.flink.table.functions.ScalarFunction;

public class VectorToDouble extends ScalarFunction {

  public double[] eval(FlinkVectorType vectorType) {
    return vectorType.getValue();
  }

}