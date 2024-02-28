package com.datasqrl.engine.log;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.engine.ExecutionEngine;

public interface LogEngine extends ExecutionEngine {

  Log createLog(String logId, NamedRelDataType schema);

}
