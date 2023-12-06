package com.datasqrl.engine.server;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigUtil;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.graphql.config.ServerConfig;
import io.vertx.core.json.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class GenericJavaServerEngineFactory implements EngineFactory {

  @Override
  public Type getEngineType() {
    return Type.SERVER;
  }

  public static ServerConfig convertServerConfig(SqrlConfig config) {
    Map<String, Object> map = SqrlConfigUtil.toMap(config,
        Function.identity(), List.of());
    JsonObject jsonObject = new JsonObject(map);
    return new ServerConfig(jsonObject);
  }
}
