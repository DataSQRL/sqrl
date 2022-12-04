package com.datasqrl.function;

import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;

import java.util.Optional;

public interface SqrlFunction {

  static Optional<SqrlFunction> unwrapSqrlFunction(SqlOperator operator) {
    if (operator instanceof BridgingSqlFunction) {
      BridgingSqlFunction flinkFnc = (BridgingSqlFunction) operator;
      if (flinkFnc.getDefinition() instanceof SqrlFunction) {
        return Optional.of((SqrlFunction) flinkFnc.getDefinition());
      }
    }
    return Optional.empty();
  }

}
