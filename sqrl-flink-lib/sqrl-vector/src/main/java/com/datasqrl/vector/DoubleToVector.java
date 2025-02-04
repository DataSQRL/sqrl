package com.datasqrl.vector;

import com.datasqrl.function.SqrlCastFunction;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a double array to a vector
 */
public class DoubleToVector extends ScalarFunction implements SqrlCastFunction {

  public FlinkVectorType eval(double[] array) {
    return new FlinkVectorType(array);
  }
}
