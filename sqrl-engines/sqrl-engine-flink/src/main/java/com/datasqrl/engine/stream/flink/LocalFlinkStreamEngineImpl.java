/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

public class LocalFlinkStreamEngineImpl extends AbstractFlinkStreamEngine {

  public LocalFlinkStreamEngineImpl(ExecutionEnvironmentFactory execFactory) {
    super(execFactory);
  }

  public FlinkStreamBuilder createJob() {
    return new FlinkStreamBuilder(this,
        execFactory.createEnvironment());
  }
}
