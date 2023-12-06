package com.datasqrl.engine.server;

import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_DEFAULT;
import static com.datasqrl.engine.server.GenericJavaServerEngine.PORT_KEY;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigUtil;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.graphql.config.ServerConfig;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.NonNull;

import java.util.Optional;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public VertxEngine initialize(@NonNull SqrlConfig config) {
    Integer port = config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get();

    return new VertxEngine(convertServerConfig(config.getSubConfig("config")), port,Optional.empty());
  }

  @VisibleForTesting
  public VertxEngine initialize(@NonNull SqrlConfig config, Vertx vertx) {
    Integer port = config.asInt(PORT_KEY).withDefault(PORT_DEFAULT).get();
    Map<String, Object> map = SqrlConfigUtil.toMap(config.getSubConfig("config"),
        Function.identity(), List.of());
    JsonObject jsonObject = new JsonObject(map);
    ServerConfig serverConfig = new ServerConfig(jsonObject);

    return new VertxEngine(serverConfig,port, Optional.of(vertx));
  }

  public ServerConfig convertServerConfig(SqrlConfig config) {
    Map<String, Object> map = SqrlConfigUtil.toMap(config,
        Function.identity(), List.of());
    JsonObject jsonObject = new JsonObject(map);
    ServerConfig serverConfig = new ServerConfig(jsonObject);
    return serverConfig;
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    public VertxEngine(@NonNull ServerConfig serverConfig, int port, Optional<Vertx> vertx) {
      super(ENGINE_NAME, port, serverConfig, vertx);
    }
  }
}
