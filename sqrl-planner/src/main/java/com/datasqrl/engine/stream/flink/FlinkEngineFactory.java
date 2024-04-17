package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.EngineFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class FlinkEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "flink";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Type getEngineType() {
    return Type.STREAM;
  }

  @Override
  public AbstractFlinkStreamEngine initialize(@NonNull EngineConfig config,
      ConnectorFactory connectorFactory) {
    return new LocalFlinkStreamEngineImpl(config);
  }

}
