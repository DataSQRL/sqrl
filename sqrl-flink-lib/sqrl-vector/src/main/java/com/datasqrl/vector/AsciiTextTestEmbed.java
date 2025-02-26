package com.datasqrl.vector;

import org.apache.flink.table.functions.ScalarFunction;

/** A unuseful embedding function counts each character (modulo 256). Used for testing only. */
public class AsciiTextTestEmbed extends ScalarFunction {

  private static final int VECTOR_LENGTH = 256;

  public FlinkVectorType eval(String text) {
    double[] vector = new double[256];
    for (char c : text.toCharArray()) {
      vector[c % VECTOR_LENGTH] += 1;
    }
    return new FlinkVectorType(vector);
  }
}
