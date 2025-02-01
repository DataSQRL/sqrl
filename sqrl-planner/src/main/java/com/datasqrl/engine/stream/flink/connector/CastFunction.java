package com.datasqrl.engine.stream.flink.connector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.UserDefinedFunction;

@AllArgsConstructor
@Getter
public class CastFunction {
  String className;
  UserDefinedFunction function;

  public String getName() {
    return getFunction().getClass().getSimpleName();
  }
}
