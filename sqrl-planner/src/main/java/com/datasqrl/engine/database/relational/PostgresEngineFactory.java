package com.datasqrl.engine.database.relational;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class PostgresEngineFactory implements DatabaseEngineFactory {

  public static final String ENGINE_NAME = "postgres";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public AbstractJDBCEngine create(@NonNull EngineConfig config,
      ConnectorFactoryFactory connectorFactoryFactory) {
    return new AbstractJDBCEngine(config, connectorFactoryFactory);
  }

  @Override
  public Class getFactoryClass() {
    return PostgresJdbcEngine.class;
  }

}
