package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.EngineFactory;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class FlinkEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "flink";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Type getEngineType() {
    return Type.STREAMS;
  }

  @Override
  public Class getFactoryClass() {
    return LocalFlinkStreamEngineImpl.class;
  }
}
