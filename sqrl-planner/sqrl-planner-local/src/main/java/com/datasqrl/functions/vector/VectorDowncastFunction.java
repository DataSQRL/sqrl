package com.datasqrl.functions.vector;

import static com.datasqrl.function.SqrlFunction.getFunctionNameFromClass;

import com.datasqrl.function.DowncastFunction;
import com.datasqrl.vector.FlinkVectorType;
import com.datasqrl.vector.VectorToDouble;
import com.google.auto.service.AutoService;

@AutoService(DowncastFunction.class)
public class VectorDowncastFunction implements DowncastFunction {
  @Override
  public Class getConversionClass() {
    return FlinkVectorType.class;
  }

  @Override
  public String downcastFunctionName() {
    return getFunctionNameFromClass(VectorToDouble.class);
  }

  @Override
  public Class getDowncastClassName() {
    return VectorToDouble.class;
  }
}
