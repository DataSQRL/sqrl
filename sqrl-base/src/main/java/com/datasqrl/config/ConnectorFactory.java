package com.datasqrl.config;

import java.util.Map;
import java.util.Optional;

public interface ConnectorFactory {
  TableConfig createSourceAndSink(IConnectorFactoryContext context);

  Optional<TableConfig.Format> getFormat();

  interface IConnectorFactoryContext {
    Map<String, Object> getMap();
  }
}
