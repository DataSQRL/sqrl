package com.datasqrl.engine.stream.flink.connector;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.sql.SqlFunction;

@AllArgsConstructor
@Getter
public class CastFunction {
  String className;
  SqlFunction function;
}
