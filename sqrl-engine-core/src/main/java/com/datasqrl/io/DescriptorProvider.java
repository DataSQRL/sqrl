/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

public interface DescriptorProvider<ENGINE_SINK, CONFIG extends DataSystemConnectorConfig> {
  ENGINE_SINK create(CONFIG config);
}
