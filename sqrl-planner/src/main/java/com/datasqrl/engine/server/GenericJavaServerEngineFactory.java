package com.datasqrl.engine.server;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.graphql.config.ServerConfig;
import io.vertx.core.json.JsonObject;
import java.util.Map;

public abstract class GenericJavaServerEngineFactory implements EngineFactory {

  @Override
  public Type getEngineType() {
    return Type.SERVER;
  }

}
