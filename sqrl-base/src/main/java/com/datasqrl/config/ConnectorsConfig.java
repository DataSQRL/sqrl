package com.datasqrl.config;

import java.util.Optional;

public interface ConnectorsConfig {

  Optional<ConnectorConf> getConnectorConfig(String name);

  ConnectorConf getConnectorConfigOrErr(String name);

}
