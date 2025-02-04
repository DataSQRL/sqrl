package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineType;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class FlinkEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "flink";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.STREAMS;
  }

  @Override
  public Class getFactoryClass() {
    return LocalFlinkStreamEngineImpl.class;
  }
}
