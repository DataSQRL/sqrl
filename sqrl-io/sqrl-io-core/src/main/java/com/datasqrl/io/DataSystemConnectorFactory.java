package com.datasqrl.io;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.util.ServiceLoaderDiscovery;
import lombok.NonNull;

public interface DataSystemConnectorFactory extends DataSystemImplementationFactory {

  DataSystemConnector initialize(@NonNull SqrlConfig connectorConfig);

  static DataSystemConnector fromConfig(@NonNull TableConfig tableConfig) {
    DataSystemConnectorFactory factory = DataSystemImplementationFactory.fromConfig(DataSystemConnectorFactory.class, tableConfig);
    return factory.initialize(tableConfig.getConnectorConfig());
  }

}
