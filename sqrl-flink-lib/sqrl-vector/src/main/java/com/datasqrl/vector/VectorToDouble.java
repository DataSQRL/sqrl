package com.datasqrl.vector;

import com.datasqrl.function.SqrlCastFunction;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a vector to a double array
 */
public class VectorToDouble extends ScalarFunction implements SqrlCastFunction {

  public double[] eval(FlinkVectorType vectorType) {
    return vectorType.getValue();
  }

}