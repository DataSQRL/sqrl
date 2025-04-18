package com.datasqrl.vector;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.types.vector.FlinkVectorType;
import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a double array to a vector
 */
@AutoService(AutoRegisterSystemFunction.class)
public class DoubleToVector extends ScalarFunction implements SqrlCastFunction, AutoRegisterSystemFunction {

  public FlinkVectorType eval(double[] array) {
    return new FlinkVectorType(array);
  }
}
