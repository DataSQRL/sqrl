package com.datasqrl.engine.database;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.EngineFactory;
import lombok.NonNull;

public interface DatabaseEngineFactory extends EngineFactory {

  @Override
  DatabaseEngine create(@NonNull EngineConfig config,
      ConnectorFactoryFactory connectorFactoryFactory);

  default EngineFactory.Type getEngineType() {
    return EngineFactory.Type.DATABASE;
  }
}
