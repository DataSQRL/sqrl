package com.datasqrl.engine.database;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineType;

public interface DatabaseEngineFactory extends EngineFactory {

  default EngineType getEngineType() {
    return EngineType.DATABASE;
  }
}
