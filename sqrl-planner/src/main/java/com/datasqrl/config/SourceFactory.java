/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

public interface SourceFactory<ENGINE_SOURCE, C extends SourceFactoryContext> extends BaseConnectorFactory {
  String getSourceName();
  ENGINE_SOURCE create(C context);
}
