/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import java.util.Optional;
import java.util.ServiceLoader;

public class SinkServiceLoader {

  public Optional<SinkFactory> load(String engineName, String sinkType) {
    ServiceLoader<SinkFactory> sinkFactories = ServiceLoader.load(SinkFactory.class);
    for (SinkFactory factory : sinkFactories) {
      if (factory.getEngine().equalsIgnoreCase(engineName)
          && factory.getSinkType().equalsIgnoreCase(sinkType)) {
        return Optional.of(factory);
      }
    }

    return Optional.empty();
  }
}
