package com.datasqrl.functions.json;

import static com.datasqrl.function.SqrlFunction.getFunctionNameFromClass;

import com.datasqrl.function.DowncastFunction;
import com.datasqrl.json.JsonToString;
import com.datasqrl.json.FlinkJsonType;
import com.google.auto.service.AutoService;

@AutoService(DowncastFunction.class)
public class JsonDowncastFunction implements DowncastFunction {
  @Override
  public Class getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public String downcastFunctionName() {
    return getFunctionNameFromClass(JsonToString.class);
  }

  @Override
  public Class getDowncastClassName() {
    return JsonToString.class;
  }
}
