package com.datasqrl.engine.database;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import lombok.NonNull;

public interface DatabaseEngineFactory extends EngineFactory {

  @Override
  DatabaseEngine initialize(@NonNull SqrlConfig config);

  default ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.DATABASE;
  }
}
