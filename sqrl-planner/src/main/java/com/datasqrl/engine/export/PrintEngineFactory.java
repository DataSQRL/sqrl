package com.datasqrl.engine.export;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.engine.IExecutionEngine;
import com.google.auto.service.AutoService;

@AutoService(EngineFactory.class)
public class PrintEngineFactory implements EngineFactory  {

  public static final String NAME = "print";

  @Override
  public String getEngineName() {
    return NAME;
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.EXPORT;
  }

  @Override
  public Class<? extends IExecutionEngine> getFactoryClass() {
    return PrintEngine.class;
  }
}
