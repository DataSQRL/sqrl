package com.datasqrl.discovery.system;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson.DataDiscoveryConfig;
import com.datasqrl.config.TableConfig;
import com.google.auto.service.AutoService;
import java.util.Collection;
import lombok.NonNull;

@AutoService(DataSystemDiscovery.class)
public class KafkaSystemDiscovery implements DataSystemDiscovery {

  public static final String TYPE = "kafka";
  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public Collection<TableConfig> discoverTables(@NonNull DataDiscoveryConfig discoveryConfig,
      @NonNull String configFile, ConnectorFactoryFactory connectorFactoryFactory) {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
