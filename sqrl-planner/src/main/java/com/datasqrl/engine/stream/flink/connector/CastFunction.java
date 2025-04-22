package com.datasqrl.engine.stream.flink.connector;

import org.apache.flink.table.functions.UserDefinedFunction;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class CastFunction {
  String className;
  UserDefinedFunction function;

  public String getName() {
    return getFunction().getClass().getSimpleName();
  }
}
