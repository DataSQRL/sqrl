package com.datasqrl.json;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.functions.SqlJsonUtils;

/**
 * For a given JSON object, checks whether the provided JSON path exists
 */
public class JsonExists extends ScalarFunction {

  public Boolean eval(FlinkJsonType json, String path) {
    if (json == null) {
      return null;
    }
    try {
      return SqlJsonUtils.jsonExists(json.json, path);
    } catch (Exception e) {
      return false;
    }
  }

}