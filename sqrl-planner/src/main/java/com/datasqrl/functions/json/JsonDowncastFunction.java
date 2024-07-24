package com.datasqrl.functions.json;

import static com.datasqrl.function.FlinkUdfNsObject.getFunctionNameFromClass;

import com.datasqrl.engine.stream.flink.connector.CastFunction;
import com.datasqrl.function.DowncastFunction;
import com.datasqrl.json.JsonToString;
import com.datasqrl.json.FlinkJsonType;
import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.calcite.sql.SqlFunction;

@AutoService(CastFunction.class)
public class JsonDowncastFunction extends CastFunction {

  public JsonDowncastFunction(String className,
      SqlFunction function) {
    super(className, function);
  }

//  @Override
//  public Class getConversionClass() {
//    return FlinkJsonType.class;
//  }
//
//  @Override
//  public String downcastFunctionName() {
//    return getFunctionNameFromClass(JsonToString.class).getDisplay();
//  }
//
//  @Override
//  public Class getDowncastClassName() {
//    return JsonToString.class;
//  }
}
