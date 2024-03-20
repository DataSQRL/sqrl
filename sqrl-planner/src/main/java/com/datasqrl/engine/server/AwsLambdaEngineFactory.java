package com.datasqrl.engine.server;

import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_DEFAULT;
import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_KEY;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.graphql.config.ServerConfig;
import com.google.auto.service.AutoService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

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
    ServerConfig serverConfig = convertServerConfig(config.getSubConfig("config"));
    return new LambdaNativeEngine(serverConfig, config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get());
  }

  public static class LambdaNativeEngine extends GenericJavaServerEngine {

    public LambdaNativeEngine(ServerConfig serverConfig, @NonNull int port) {
      super(ENGINE_NAME, port, serverConfig, Optional.empty());
    }
  }
}
