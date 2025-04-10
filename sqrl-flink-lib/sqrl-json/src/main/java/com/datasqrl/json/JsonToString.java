package com.datasqrl.json;

import org.apache.flink.table.functions.ScalarFunction;

import com.datasqrl.function.SqrlCastFunction;
import com.datasqrl.types.json.FlinkJsonType;

public class JsonToString extends ScalarFunction implements SqrlCastFunction {

  public String eval(FlinkJsonType json) {
    if (json == null) {
      return null;
    }
    return json.getJson().toString();
  }

}