package com.datasqrl.json;

import org.apache.flink.table.functions.ScalarFunction;

public class JsonToString extends ScalarFunction {

  public String eval(FlinkJsonType json) {
    if (json == null) {
      return null;
    }
    return json.getJson().toString();
  }
}
