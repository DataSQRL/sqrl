package com.datasqrl.flink.function;

import org.apache.flink.table.functions.FunctionDefinition;

public interface BridgingFunction {

  FunctionDefinition getDefinition();
}
