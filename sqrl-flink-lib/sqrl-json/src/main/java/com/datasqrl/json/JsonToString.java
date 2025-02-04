package com.datasqrl.json;

import com.datasqrl.function.SqrlCastFunction;
import org.apache.flink.table.functions.ScalarFunction;

public class JsonToString extends ScalarFunction implements SqrlCastFunction {

  public String eval(FlinkJsonType json) {
    if (json == null) {
      return null;
    }
    return json.getJson().toString();
  }

}