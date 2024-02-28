/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream;

import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.ExecutionEngine;
import java.io.Closeable;

public interface StreamEngine extends Closeable, ExecutionEngine {

//  /**
//   * This method must be implemented if the engine supports {@link EngineFeature#DATA_MONITORING}
//   * otherwise it can be ignored.
//   *
//   * @return
//   */
//  default DataMonitor createDataMonitor() {
//    throw new UnsupportedOperationException("Capability not supported by engine");
//  }

}
