/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SqrlConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  public LocalFlinkStreamEngineImpl(SqrlConfig config) {
    super(config);
  }
}