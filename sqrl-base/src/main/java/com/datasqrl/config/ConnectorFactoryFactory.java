package com.datasqrl.config;

import com.datasqrl.config.EngineFactory.Type;
import java.util.Optional;

public interface ConnectorFactoryFactory {

  Optional<ConnectorFactory> create(Type engineType, String connectorName);

  Optional<ConnectorFactory> create(SystemBuiltInConnectors builtInConnector);

  ConnectorConf getConfig(String name);


}
