package com.datasqrl.engine.log;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExportEngine;

public interface LogEngine extends ExportEngine {

  LogFactory getLogFactory();

}
