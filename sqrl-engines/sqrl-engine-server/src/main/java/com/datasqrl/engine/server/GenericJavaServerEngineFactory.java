package com.datasqrl.engine.server;

import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine.Type;

public abstract class GenericJavaServerEngineFactory implements EngineFactory {

  @Override
  public Type getEngineType() {
    return Type.SERVER;
  }

}
