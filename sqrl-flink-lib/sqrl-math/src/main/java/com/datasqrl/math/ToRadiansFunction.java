package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.commons.math3.util.FastMath;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class ToRadiansFunction extends ScalarFunction{
  public double eval(double x) {
    return FastMath.toRadians(x);
  }
}
