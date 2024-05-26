package com.datasqrl.engine.server;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.IExecutionEngine;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkSqlGenerator;
import com.google.auto.service.AutoService;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Class<? extends IExecutionEngine> getFactoryClass() {
    return VertxEngine.class;
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    @Inject
    public VertxEngine(ConnectorFactoryFactory connectorFactory, SqrlToFlinkSqlGenerator flinkSqlGenerator) {
      super(ENGINE_NAME, connectorFactory, flinkSqlGenerator);
    }
  }
}
