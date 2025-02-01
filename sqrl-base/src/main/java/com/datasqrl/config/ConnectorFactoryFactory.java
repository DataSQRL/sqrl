package com.datasqrl.config;

import java.util.Optional;

public interface ConnectorFactoryFactory {

  Optional<ConnectorFactory> create(EngineType engineType, String connectorName);

  Optional<ConnectorFactory> create(SystemBuiltInConnectors builtInConnector);

  ConnectorConf getConfig(String name);


}
