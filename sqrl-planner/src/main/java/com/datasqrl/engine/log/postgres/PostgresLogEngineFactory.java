package com.datasqrl.engine.log.postgres;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.IExecutionEngine;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class PostgresLogEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "postgres_log";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Type getEngineType() {
    return Type.LOG;
  }

  @Override
  public Class<? extends IExecutionEngine> getFactoryClass() {
    return PostgresLogEngine.class;
  }
}
