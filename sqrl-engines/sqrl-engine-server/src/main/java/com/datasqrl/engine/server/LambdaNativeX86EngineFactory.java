package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.server.LambdaNativeArm64EngineFactory.LambdaNativeArm64Engine;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class LambdaNativeX86EngineFactory extends GraphqlServerEngineFactory {

  public static final String ENGINE_NAME = "aws-lambda-native-x86";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new LambdaNativeX86Engine();
  }

  public class LambdaNativeX86Engine extends GenericJavaServerEngine {

    public LambdaNativeX86Engine() {
      super(ENGINE_NAME);
    }
  }
}
