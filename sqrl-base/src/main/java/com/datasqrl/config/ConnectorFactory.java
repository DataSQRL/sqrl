package com.datasqrl.config;

import java.util.Map;
import java.util.Optional;

public interface ConnectorFactory {
  TableConfig createSourceAndSink(IConnectorFactoryContext context);

  interface IConnectorFactoryContext {
    Map<String, Object> getMap();
  }
}
