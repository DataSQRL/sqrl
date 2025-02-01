package com.datasqrl.engine.server;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineType;

public abstract class GenericJavaServerEngineFactory implements EngineFactory {

  @Override
  public EngineType getEngineType() {
    return EngineType.SERVER;
  }

}
