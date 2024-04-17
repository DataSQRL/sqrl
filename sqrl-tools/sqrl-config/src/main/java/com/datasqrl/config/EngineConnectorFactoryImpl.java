package com.datasqrl.config;

import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EngineConnectorFactoryImpl implements ConnectorFactory {
  PackageJson.EngineConfig engineConfig;
  @Override
  public TableConfig createSourceAndSink(IConnectorFactoryContext context) {
    throw new RuntimeException("TBD");
  }

  @Override
  public Optional<TableConfig.Format> getFormat() {
    return Optional.empty();
  }
}
