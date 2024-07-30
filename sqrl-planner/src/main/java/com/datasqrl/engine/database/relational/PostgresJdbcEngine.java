package com.datasqrl.engine.database.relational;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.google.inject.Inject;
import lombok.NonNull;

public class PostgresJdbcEngine extends AbstractJDBCEngine {

  @Inject
  public PostgresJdbcEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(PostgresEngineFactory.ENGINE_NAME, json.getEngines().getEngineConfig(PostgresEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(PostgresEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }
}
