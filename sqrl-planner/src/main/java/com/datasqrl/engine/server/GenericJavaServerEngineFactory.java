package com.datasqrl.engine.server;

import com.datasqrl.config.EngineFactory;

public abstract class GenericJavaServerEngineFactory implements EngineFactory {

  @Override
  public Type getEngineType() {
    return Type.SERVER;
  }
}
