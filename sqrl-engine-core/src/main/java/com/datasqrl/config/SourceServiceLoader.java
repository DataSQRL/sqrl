/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import java.util.Optional;
import java.util.ServiceLoader;

public class SourceServiceLoader {

  public Optional<SourceFactory> load(String engine, String source) {
    ServiceLoader<SourceFactory> sourceFactories = ServiceLoader.load(SourceFactory.class);
    for (SourceFactory factory : sourceFactories) {
      if (factory.getEngine().equalsIgnoreCase(engine)
          && factory.getSourceName().equalsIgnoreCase(source)) {
        return Optional.of(factory);
      }
    }

    return Optional.empty();
  }

  public interface SourceFactoryContext {

  }
}
