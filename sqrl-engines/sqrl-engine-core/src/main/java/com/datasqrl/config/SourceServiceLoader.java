/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.util.ServiceLoaderDiscovery;

import java.util.Optional;

public class SourceServiceLoader {

  public Optional<SourceFactory> load(String engine, String source) {
    return ServiceLoaderDiscovery.findFirst(SourceFactory.class, sf -> sf.getEngine(), engine,
            sf -> sf.getSourceName(), source);
  }

}
