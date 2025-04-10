package com.datasqrl.json;

import com.datasqrl.function.AutoRegisterSystemFunction;
import com.datasqrl.types.json.FlinkJsonType;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;

/** For a given JSON object, checks whether the provided JSON path exists */
@AutoService(AutoRegisterSystemFunction.class)
public class JsonExists extends ScalarFunction implements AutoRegisterSystemFunction {

  public Boolean eval(FlinkJsonType json, String path) {
    if (json == null) {
      return null;
    }
    try {
      return SqlJsonUtils.jsonExists(json.json.toString(), path);
    } catch (Exception e) {
      return false;
    }
  }
}
