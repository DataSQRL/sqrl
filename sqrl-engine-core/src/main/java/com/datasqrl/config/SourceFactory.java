/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.config.SourceServiceLoader.SourceFactoryContext;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.plan.global.OptimizedDAG.WriteSink;

public interface SourceFactory<ENGINE_SOURCE> {
  String getEngine();
  String getSourceName();

  ENGINE_SOURCE create(DataSystemConnector connector, SourceFactoryContext context);
}
