package com.datasqrl.engine.database;

import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.EngineFactory;
import lombok.NonNull;

public interface DatabaseEngineFactory extends EngineFactory {

  @Override
  DatabaseEngine initialize(@NonNull EngineConfig config, ConnectorFactory connectorFactory);

  default EngineFactory.Type getEngineType() {
    return EngineFactory.Type.DATABASE;
  }
}
