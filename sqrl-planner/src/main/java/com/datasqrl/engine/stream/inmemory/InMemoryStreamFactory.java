package com.datasqrl.engine.stream.inmemory;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.error.ErrorCollector;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class InMemoryStreamFactory implements EngineFactory {

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
  public InMemStreamEngine initialize(@NonNull SqrlConfig config) {
    return new InMemStreamEngine();
  }

}
