/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigurationUtil;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FlinkEngineConfiguration implements EngineConfiguration {

  public static final String ENGINE_NAME = "flink";

  @Builder.Default
  boolean savepoint = false;

  @Builder.Default
  private Map<String, String> flinkConf = new HashMap<>();

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.STREAM;
  }

  @Override
  public AbstractFlinkStreamEngine initialize(@NonNull ErrorCollector errors) {
    ConfigurationUtil.javaxValidate(this, errors);
    return new LocalFlinkStreamEngineImpl(this);
  }

  public ExecutionEnvironmentFactory createEnvFactory() {
    return new ExecutionEnvironmentFactory(flinkConf);
  }
}
