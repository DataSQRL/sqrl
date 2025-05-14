package com.datasqrl.engine;

import com.datasqrl.config.EngineType;

public interface IExecutionEngine {
  String getName();
  EngineType getType();
}
