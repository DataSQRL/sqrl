package com.datasqrl.config;

import java.util.Optional;

import com.datasqrl.config.EngineFactory.Type;

public interface ConnectorFactoryFactory {

  Optional<ConnectorFactory> create(Type engineType, String connectorName);

  Optional<ConnectorFactory> create(SystemBuiltInConnectors builtInConnector);

  ConnectorConf getConfig(String name);


}
