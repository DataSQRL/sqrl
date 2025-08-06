package com.myudf;

import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class MyScalarFunction extends ScalarFunction {

  public Long eval(Long a, Long b) {
    return a + b;
  }
}
