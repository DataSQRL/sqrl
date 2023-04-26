package com.datasqrl.engine.server;

import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine.Type;

public abstract class GraphqlServerEngineFactory implements EngineFactory {
  public static final String ENGINE_NAME_KEY = "name";

  @Override
  public Type getEngineType() {
    return Type.SERVER;
  }

}
