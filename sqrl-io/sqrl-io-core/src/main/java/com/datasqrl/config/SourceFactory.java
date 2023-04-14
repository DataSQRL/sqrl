/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.io.DataSystemConnector;

public interface SourceFactory<ENGINE_SOURCE> extends BaseConnectorFactory {
  String getSourceName();
  ENGINE_SOURCE create(DataSystemConnector connector, SourceFactoryContext context);
}
