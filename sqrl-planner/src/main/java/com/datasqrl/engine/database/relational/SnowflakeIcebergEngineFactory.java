package com.datasqrl.engine.database.relational;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class SnowflakeIcebergEngineFactory implements DatabaseEngineFactory {

  public static final String ENGINE_NAME = "snowflake";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Class getFactoryClass() {
    return SnowflakeIcebergEngine.class;
  }

}
