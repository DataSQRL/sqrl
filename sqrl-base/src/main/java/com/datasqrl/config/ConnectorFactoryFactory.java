package com.datasqrl.config;

import com.datasqrl.config.EngineFactory.Type;
import java.util.Optional;

public interface ConnectorFactoryFactory {
  public static final String PRINT_SINK_NAME = "print";
  public static final String FILE_SINK_NAME = "localfile";

  Optional<ConnectorFactory> create(Type type, String engineId);
  ConnectorConf getConfig(String name);

}
