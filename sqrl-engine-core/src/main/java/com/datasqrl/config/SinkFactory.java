/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.plan.global.OptimizedDAG.EngineSink;

public interface SinkFactory<ENGINE_SINK> {
  String getEngine();
  String getSinkName();

  ENGINE_SINK create(EngineSink sink);
}
