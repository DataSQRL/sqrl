package com.datasqrl.config;

import java.util.Optional;

public interface ConnectorFactoryFactory {

  ConnectorFactory create(String engineId, PackageJson.EngineConfig engineConfig);

  Optional<TableConfig.Format> getFormatForExtension(String format);
}
