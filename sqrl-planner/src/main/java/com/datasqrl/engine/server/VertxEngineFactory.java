package com.datasqrl.engine.server;

import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.PackageJson.EngineConfig;

import com.datasqrl.config.EngineFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public VertxEngine initialize(@NonNull EngineConfig config, ConnectorFactory connectorFactory) {
    return new VertxEngine();
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine() {
      super(ENGINE_NAME);
    }
  }
}
