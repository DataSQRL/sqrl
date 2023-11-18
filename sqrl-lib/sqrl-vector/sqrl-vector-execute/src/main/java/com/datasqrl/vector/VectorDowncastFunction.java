package com.datasqrl.vector;

import com.datasqrl.function.DowncastFunction;
import com.google.auto.service.AutoService;

@AutoService(DowncastFunction.class)
public class VectorDowncastFunction implements DowncastFunction {
  @Override
  public Class getConversionClass() {
    return FlinkVectorType.class;
  }

  @Override
  public String downcastFunctionName() {
    return "VectorToDouble";
  }
}
