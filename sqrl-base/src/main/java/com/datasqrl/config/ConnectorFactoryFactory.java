package com.datasqrl.config;

import com.datasqrl.config.EngineFactory.Type;
import java.util.Optional;

public interface ConnectorFactoryFactory {
  String PRINT_SINK_NAME = "print";
  String FILE_SINK_NAME = "localfile";
  String LOG_SINK_NAME = "log";

  Optional<ConnectorFactory> create(Type type, String engineId);
  ConnectorConf getConfig(String name);

}
