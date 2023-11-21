package com.datasqrl.function;

public interface DowncastFunction {

  Class getConversionClass();

  String downcastFunctionName();

  Class getDowncastClassName();
}
