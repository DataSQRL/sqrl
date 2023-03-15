/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  public LocalFlinkStreamEngineImpl(FlinkEngineConfiguration config) {
    super(config);
  }

  public FlinkStreamBuilder createJob() {
    return new FlinkStreamBuilder(this,
        config.createEnvFactory().createEnvironment());
  }
}
