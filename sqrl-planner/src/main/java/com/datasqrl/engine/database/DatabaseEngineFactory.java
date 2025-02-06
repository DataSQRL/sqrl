package com.datasqrl.engine.database;

import com.datasqrl.config.EngineFactory;

public interface DatabaseEngineFactory extends EngineFactory {

  @Override
default EngineFactory.Type getEngineType() {
    return EngineFactory.Type.DATABASE;
  }
}
