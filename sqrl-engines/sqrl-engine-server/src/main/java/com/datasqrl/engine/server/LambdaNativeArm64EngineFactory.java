package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.server.LambdaJavaEngineFactory.LambdaJavaEngine;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class LambdaNativeArm64EngineFactory extends GraphqlServerEngineFactory {

  public static final String ENGINE_NAME = "aws-lambda-native-arm64";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new LambdaNativeArm64Engine();
  }

  public class LambdaNativeArm64Engine extends GenericJavaServerEngine {

    public LambdaNativeArm64Engine() {
      super(ENGINE_NAME);
    }
  }
}
