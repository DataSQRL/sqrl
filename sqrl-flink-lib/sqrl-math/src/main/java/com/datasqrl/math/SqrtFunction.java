package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class SqrtFunction extends ScalarFunction {
  public double eval(double x) {
    return FastMath.sqrt(x);
  }
}
