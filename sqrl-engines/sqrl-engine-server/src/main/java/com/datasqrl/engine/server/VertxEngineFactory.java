package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.server.LambdaNativeX86EngineFactory.LambdaNativeX86Engine;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GraphqlServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new VertxEngine();
  }

  public class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine() {
      super(ENGINE_NAME);
    }
  }
}
