package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class MinFunction extends ScalarFunction {
  public int eval(int a, int b) {
    return FastMath.min(a, b);
  }

  public long eval(long a, long b) {
    return FastMath.min(a, b);
  }

  public float eval(float a, float b) {
    return FastMath.min(a, b);
  }

  public double eval(double a, double b) {
    return FastMath.min(a, b);
  }
}
