/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

public interface SinkFactory<ENGINE_SINK, C extends SinkFactoryContext> extends BaseConnectorFactory {
  String getSinkType();

  ENGINE_SINK create(C context);

}
