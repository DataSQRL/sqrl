package com.datasqrl.functions.json;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionNameFromClass;

import com.datasqrl.function.DowncastFunction;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.json.JsonToString;
import com.datasqrl.json.ToJson;
import com.google.auto.service.AutoService;

@AutoService(DowncastFunction.class)
public class RowToJsonDowncastFunction implements DowncastFunction {
  @Override
  public Class getConversionClass() {
    return FlinkJsonType.class;
  }

  @Override
  public String downcastFunctionName() {
    return getFunctionNameFromClass(ToJson.class).getDisplay();
  }

  @Override
  public Class getDowncastClassName() {
    return ToJson.class;
  }
}