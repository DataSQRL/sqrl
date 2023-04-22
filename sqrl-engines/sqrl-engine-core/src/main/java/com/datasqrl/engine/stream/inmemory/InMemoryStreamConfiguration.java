/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.inmemory;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.ExecutionEngine;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * TODO: remove, replaced by factory
 */
@Builder
@Getter
@NoArgsConstructor
public class InMemoryStreamConfiguration implements EngineConfiguration {

  public static final String ENGINE_NAME = "memStream";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public ExecutionEngine.Type getEngineType() {
    return ExecutionEngine.Type.STREAM;
  }

  @Override
  public InMemStreamEngine initialize(@NonNull ErrorCollector errors) {
    return new InMemStreamEngine();
  }
}
