package com.datasqrl.vector;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a double array to a vector
 */
@AutoService(StandardLibraryFunction.class)
public class DoubleToVector extends ScalarFunction implements SqrlCastFunction, StandardLibraryFunction {

  public FlinkVectorType eval(double[] array) {
    return new FlinkVectorType(array);
  }
}
