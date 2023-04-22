package com.datasqrl.io;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Set;
import lombok.NonNull;

public interface DataSystemImplementationFactory {

  String SYSTEM_NAME_KEY = "name";

  String getSystemName();

  static<F extends DataSystemImplementationFactory> F fromConfig(Class<F> clazz,
      @NonNull TableConfig config) {
    SqrlConfig connectorConfig = config.getConnectorConfig();
    return ServiceLoaderDiscovery.get(clazz, DataSystemImplementationFactory::getSystemName,
        connectorConfig.asString(SYSTEM_NAME_KEY).get());
  }

}
