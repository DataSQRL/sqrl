package com.datasqrl.engine.log.postgres;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineType;
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
  public EngineType getEngineType() {
    return EngineType.LOG;
  }

  @Override
  public Class<? extends IExecutionEngine> getFactoryClass() {
    return PostgresLogEngine.class;
  }
}
