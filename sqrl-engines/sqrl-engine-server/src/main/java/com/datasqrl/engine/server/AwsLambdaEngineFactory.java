package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.google.auto.service.AutoService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(EngineFactory.class)
public class AwsLambdaEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "aws-lambda";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new LambdaNativeEngine(config);
  }

  public static class LambdaNativeEngine extends GenericJavaServerEngine {

    public LambdaNativeEngine(@NonNull SqrlConfig config) {
      super(ENGINE_NAME, config);
    }
  }
}
