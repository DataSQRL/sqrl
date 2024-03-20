package com.datasqrl.io;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.util.ServiceLoaderDiscovery;
import lombok.NonNull;

public interface DynamicSinkConnectorFactory {

  ConnectorConfig forName(@NonNull Name name, @NonNull SqrlConfig baseConnectorConfig);

  public String getType();

  static DynamicSinkConnectorFactory load(String type) {
    return ServiceLoaderDiscovery.get(DynamicSinkConnectorFactory.class, DynamicSinkConnectorFactory::getType, type);
  }

}
