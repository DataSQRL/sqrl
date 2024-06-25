/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  @Inject
  public LocalFlinkStreamEngineImpl(PackageJson json) {
    super(json.getEngines().getEngineConfig(FlinkEngineFactory.ENGINE_NAME)
        .orElseGet(()->new EmptyEngineConfig(FlinkEngineFactory.ENGINE_NAME)));
  }
}
