package com.datasqrl.io;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.tables.TableConfig;
import lombok.NonNull;

public interface DataSystemConnectorFactory extends DataSystemImplementationFactory {

  DataSystemConnectorSettings getSettings(@NonNull SqrlConfig connectorConfig);

}
