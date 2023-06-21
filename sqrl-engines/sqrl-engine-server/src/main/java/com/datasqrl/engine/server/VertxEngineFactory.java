package com.datasqrl.engine.server;

import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_DEFAULT;
import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_KEY;

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
  public VertxEngine initialize(@NonNull SqrlConfig config) {
    return new VertxEngine(config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get());
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine(@NonNull int port) {
      super(ENGINE_NAME, port);
    }
  }
}
