package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import java.util.Map;

public interface ConnectorFactory {
  TableConfig createSourceAndSink(IConnectorFactoryContext context);

  interface IConnectorFactoryContext {
    Map<String, Object> getMap();

    Name getName();
  }
}
