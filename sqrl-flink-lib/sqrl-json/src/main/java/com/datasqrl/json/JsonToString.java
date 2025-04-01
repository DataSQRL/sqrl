package com.datasqrl.json;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.function.StandardLibraryFunction;
import com.google.auto.service.AutoService;

import org.apache.flink.table.functions.ScalarFunction;

@AutoService(StandardLibraryFunction.class)
public class JsonToString extends ScalarFunction implements SqrlCastFunction, StandardLibraryFunction{

  public String eval(FlinkJsonType json) {
    if (json == null) {
      return null;
    }
    return json.getJson().toString();
  }

}