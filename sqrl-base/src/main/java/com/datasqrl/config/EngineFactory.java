package com.datasqrl.config;

import com.datasqrl.engine.IExecutionEngine;
import java.util.Set;
import lombok.NonNull;

public interface EngineFactory {
  String ENGINE_NAME_KEY = "type";
  Set<String> RESERVED_KEYS = Set.of(ENGINE_NAME_KEY);

  static Set<String> getReservedKeys() {
    return RESERVED_KEYS;
  }

  String getEngineName();

  Type getEngineType();

  Class<? extends IExecutionEngine> getFactoryClass();

  enum Type {
    STREAMS, DATABASE, SERVER, LOG, QUERY, EXPORT;

    public boolean isWrite() {
      return this == STREAMS;
    }

    public boolean isRead() {
      return this == DATABASE || this == SERVER;
    }

    public boolean isCompute() { return isWrite() || isRead(); }
  }
}
