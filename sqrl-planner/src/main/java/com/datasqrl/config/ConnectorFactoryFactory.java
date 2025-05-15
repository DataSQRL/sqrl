package com.datasqrl.config;

import java.util.Optional;

public interface ConnectorFactoryFactory {

  ConnectorConf getConfig(String name);

  Optional<ConnectorConf> getOptionalConfig(String name);


}
