package com.datasqrl.engine;

import com.datasqrl.config.EngineFactory.Type;

public interface IExecutionEngine {
  String getName();
  Type getType();
}
