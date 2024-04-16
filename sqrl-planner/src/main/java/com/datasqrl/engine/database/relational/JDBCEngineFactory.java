package com.datasqrl.engine.database.relational;

import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(EngineFactory.class)
public class JDBCEngineFactory implements DatabaseEngineFactory {

  public static final String ENGINE_NAME = "jdbc";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public JDBCEngine initialize(@NonNull EngineConfig config, ConnectorFactory connectorFactory) {
    return new JDBCEngine(config, connectorFactory);
  }

}
