package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.time.Duration;
import lombok.NonNull;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.utility.MountableFile;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new VertxEngine(config);
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine(@NonNull SqrlConfig config) {
      super(ENGINE_NAME, config);
    }
  }
}
