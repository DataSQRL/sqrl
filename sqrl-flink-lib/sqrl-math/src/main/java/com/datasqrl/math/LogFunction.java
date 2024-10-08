package com.datasqrl.math;

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class LogFunction extends ScalarFunction {
  public double eval(double x) {
    return Math.log(x);
  }
}
