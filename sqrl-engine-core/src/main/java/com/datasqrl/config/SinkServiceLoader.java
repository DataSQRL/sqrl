/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.loaders.ServiceLoaderDiscovery;

import java.util.Optional;

public class SinkServiceLoader {

  public Optional<SinkFactory> load(String engineName, String sinkType) {
    return ServiceLoaderDiscovery.findFirst(SinkFactory.class, sf -> sf.getEngine(), engineName,
            sf -> sf.getSinkType(), sinkType);
  }
}
