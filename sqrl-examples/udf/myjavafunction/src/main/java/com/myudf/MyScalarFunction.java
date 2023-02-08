package com.myudf;

import com.datasqrl.function.SqrlFunction;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class MyScalarFunction extends ScalarFunction implements SqrlFunction {

  public long eval(long a, long b) {
    return a + b;
  }
}
