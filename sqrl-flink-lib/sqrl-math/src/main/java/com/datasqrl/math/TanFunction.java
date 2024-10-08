package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class TanFunction extends ScalarFunction {
  public double fastTan(double x) {
    return FastMath.tan(x);
  }
}
