package com.datasqrl.config;

import java.util.Map;

import com.datasqrl.canonicalizer.Name;

public interface ConnectorFactory {
  TableConfig createSourceAndSink(IConnectorFactoryContext context);

  interface IConnectorFactoryContext {
    Map<String, Object> getMap();
    Name getName();
  }
}
