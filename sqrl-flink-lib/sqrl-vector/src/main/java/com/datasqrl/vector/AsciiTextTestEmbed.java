package com.datasqrl.vector;

import org.apache.flink.table.functions.ScalarFunction;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.google.auto.service.AutoService;

/**
 * A unuseful embedding function counts each character (modulo 256). Used for testing only.
 */
@AutoService(AutoRegisterSystemFunction.class)
public class AsciiTextTestEmbed extends ScalarFunction implements AutoRegisterSystemFunction {

  private static final int VECTOR_LENGTH = 256;

  public FlinkVectorType eval(String text) {
    double[] vector = new double[256];
    for (char c : text.toCharArray()) {
      vector[c % VECTOR_LENGTH] += 1;
    }
    return new FlinkVectorType(vector);
  }

}
