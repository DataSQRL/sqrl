package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class LambdaJavaEngineFactory extends GraphqlServerEngineFactory {

  public static final String ENGINE_NAME = "aws-lambda-native";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new LambdaJavaEngine();
  }

  public class LambdaJavaEngine extends GenericJavaServerEngine {

    public LambdaJavaEngine() {
      super(ENGINE_NAME);
    }
  }
}
