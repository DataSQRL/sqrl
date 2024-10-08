package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class AbsFunction extends ScalarFunction {
  public int eval(int x) {
    return FastMath.abs(x);
  }

  public long eval(long x) {
    return FastMath.abs(x);
  }

  public float eval(float x) {
    return FastMath.abs(x);
  }

  public double eval(double x) {
    return FastMath.abs(x);
  }
}
