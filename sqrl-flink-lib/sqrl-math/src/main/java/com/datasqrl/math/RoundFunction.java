package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class RoundFunction extends ScalarFunction {
  public double eval(float x) {
    return FastMath.round(x);
  }

  public double eval(double x) {
    return FastMath.round(x);
  }
}
