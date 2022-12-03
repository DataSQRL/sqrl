package com.datasqrl.function.builtin.time;

import lombok.Value;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class FlinkFnc {

  String name;
  UserDefinedFunction fnc;
}
