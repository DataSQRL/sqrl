package com.datasqrl.vector;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.types.vector.FlinkVectorType;
import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Converts a vector to a double array
 */
@AutoService(AutoRegisterSystemFunction.class)
public class VectorToDouble extends ScalarFunction implements SqrlCastFunction, AutoRegisterSystemFunction {

  public double[] eval(FlinkVectorType vectorType) {
    return vectorType.getValue();
  }

}