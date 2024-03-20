package com.datasqrl.discovery.system;

import com.datasqrl.discovery.DataDiscoveryConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
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

  Collection<TableConfig> discoverTables(@NonNull DataDiscoveryConfig discoveryConfig, @NonNull String configFile);

  static Optional<DataSystemDiscovery> load(String systemType) {
    return ServiceLoaderDiscovery.findFirst(DataSystemDiscovery.class, DataSystemDiscovery::getType, systemType);
  }

  static List<String> getAvailable() {
    return ServiceLoaderDiscovery.getAll(DataSystemDiscovery.class).stream()
        .map(DataSystemDiscovery::getType).collect(Collectors.toUnmodifiableList());
  }


}
