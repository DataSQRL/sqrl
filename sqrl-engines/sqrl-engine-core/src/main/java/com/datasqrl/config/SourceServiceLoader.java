/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.util.ServiceLoaderDiscovery;

import java.util.Optional;

public class SourceServiceLoader {

  public SourceFactory load(String engine, String source) {
    return ServiceLoaderDiscovery.get(SourceFactory.class, SourceFactory::getEngine, engine,
            SourceFactory::getSourceName, source);
  }

}
