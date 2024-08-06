package com.datasqrl.engine.server;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.IExecutionEngine;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Class<? extends IExecutionEngine> getFactoryClass() {
    return VertxEngine.class;
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine() {
      super(ENGINE_NAME);
    }
  }
}
