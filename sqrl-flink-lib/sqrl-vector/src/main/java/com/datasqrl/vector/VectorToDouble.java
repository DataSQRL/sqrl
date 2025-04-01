package com.datasqrl.vector;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a vector to a double array
 */
@AutoService(StandardLibraryFunction.class)
public class VectorToDouble extends ScalarFunction implements SqrlCastFunction, StandardLibraryFunction {

  public double[] eval(FlinkVectorType vectorType) {
    return vectorType.getValue();
  }

}