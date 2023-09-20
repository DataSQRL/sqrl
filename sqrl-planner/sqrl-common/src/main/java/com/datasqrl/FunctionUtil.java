package com.datasqrl;

import com.datasqrl.flink.function.BridgingFunction;
import com.datasqrl.function.SqrlFunction;
import java.util.Optional;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;

/**
 * TODO: This class contains duplicated functions from SqrlRexUtil.
 */
public class FunctionUtil {

  public static Optional<SqrlFunction> getSqrlFunction(SqlOperator operator) {
    if (operator instanceof BridgingFunction) {
      FunctionDefinition function = ((BridgingFunction)operator).getDefinition();
      if (function instanceof SqrlFunction) {
        return Optional.of((SqrlFunction) function);
      }
    }
    return Optional.empty();
  }

}
