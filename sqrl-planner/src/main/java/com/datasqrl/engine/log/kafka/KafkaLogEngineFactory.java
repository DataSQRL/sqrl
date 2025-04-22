package com.datasqrl.engine.log.kafka;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.engine.IExecutionEngine;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class KafkaLogEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "kafka";

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
    return KafkaLogEngine.class;
  }
}
