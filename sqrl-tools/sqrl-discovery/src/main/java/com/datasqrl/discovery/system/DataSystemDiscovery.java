package com.datasqrl.discovery.system;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson.DataDiscoveryConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.NonNull;

public interface DataSystemDiscovery {

  String getType();

  default boolean matchesArgument(String systemConfig) {
    return false;
  }

  Collection<TableConfig> discoverTables(@NonNull DataDiscoveryConfig discoveryConfig, @NonNull String configFile,
      ConnectorFactoryFactory connectorFactoryFactory);

  static Optional<DataSystemDiscovery> load(String systemType) {
    return ServiceLoaderDiscovery.findFirst(DataSystemDiscovery.class, DataSystemDiscovery::getType, systemType);
  }

  static List<String> getAvailable() {
    return ServiceLoaderDiscovery.getAll(DataSystemDiscovery.class).stream()
        .map(DataSystemDiscovery::getType).collect(Collectors.toUnmodifiableList());
  }
}