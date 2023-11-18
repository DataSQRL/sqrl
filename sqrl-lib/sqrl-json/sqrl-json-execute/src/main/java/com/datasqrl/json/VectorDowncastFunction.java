package com.datasqrl.json;

import com.datasqrl.function.DowncastFunction;
import com.google.auto.service.AutoService;

@AutoService(DowncastFunction.class)
public class VectorDowncastFunction implements DowncastFunction {
  @Override
  public Class getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public String downcastFunctionName() {
    return "jsontostring";
  }
}
