package com.datasqrl.config;

import com.datasqrl.engine.IExecutionEngine;
import java.util.Set;

public interface EngineFactory {
  String ENGINE_NAME_KEY = "type";
  Set<String> RESERVED_KEYS = Set.of(ENGINE_NAME_KEY);

  static Set<String> getReservedKeys() {
    return RESERVED_KEYS;
  }

  String getEngineName();

  EngineType getEngineType();

  Class<? extends IExecutionEngine> getFactoryClass();

}
