package com.datasqrl.vector;

import com.datasqrl.function.DowncastFunction;
import com.datasqrl.vector.VectorFunctions.VectorToDouble;
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

  @Override
  public Class getDowncastClassName() {
    return VectorToDouble.class;
  }
}
