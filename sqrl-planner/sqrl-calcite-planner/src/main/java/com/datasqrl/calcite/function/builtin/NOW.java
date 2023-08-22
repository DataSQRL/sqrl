
package com.datasqrl.calcite.function.builtin;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.Instant;

public class NOW extends ScalarFunction {

  public Instant eval() {
    return Instant.now();
  }

}