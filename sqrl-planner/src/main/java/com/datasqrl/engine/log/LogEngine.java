package com.datasqrl.engine.log;

import com.datasqrl.engine.ExecutionEngine;

public interface LogEngine extends ExecutionEngine {

  LogFactory getLogFactory();

}
