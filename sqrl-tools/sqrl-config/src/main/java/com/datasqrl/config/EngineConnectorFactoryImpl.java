package com.datasqrl.config;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EngineConnectorFactoryImpl implements ConnectorFactory {
  PackageJson.EngineConfig engineConfig;
  @Override
  public TableConfig createSourceAndSink(IConnectorFactoryContext context) {
    throw new RuntimeException("TBD");
  }
}
