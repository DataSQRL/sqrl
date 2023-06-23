package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import lombok.NonNull;

import java.util.Optional;

import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_DEFAULT;
import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_KEY;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public VertxEngine initialize(@NonNull SqrlConfig config) {
    return new VertxEngine(config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get(), Optional.empty());
  }

  @VisibleForTesting
  public VertxEngine initialize(@NonNull SqrlConfig config, Vertx vertx) {
    return new VertxEngine(config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get(), Optional.of(vertx));
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine(@NonNull int port, Optional<Vertx> vertx) {
      super(ENGINE_NAME, port, vertx);
    }
  }
}
