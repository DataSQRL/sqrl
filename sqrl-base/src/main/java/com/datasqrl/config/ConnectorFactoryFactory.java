package com.datasqrl.config;

import java.util.Optional;

public interface ConnectorFactoryFactory {
  public static final String PRINT_SINK_NAME = "print";
  public static final String FILE_SINK_NAME = "file";

  ConnectorFactory create(String engineId, PackageJson.EngineConfig engineConfig);

  Optional<TableConfig.Format> getFormatForExtension(String format);
}
