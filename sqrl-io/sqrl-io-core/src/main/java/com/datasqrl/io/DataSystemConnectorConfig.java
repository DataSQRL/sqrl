/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.error.ErrorCollector;
import lombok.NonNull;

/**
 * The configuration of a data source that DataSQRL can connect to for data access
 */
public interface DataSystemConnectorConfig extends DataSystemSerializableConfig {

  DataSystemConnector initialize(@NonNull ErrorCollector errors);

}
