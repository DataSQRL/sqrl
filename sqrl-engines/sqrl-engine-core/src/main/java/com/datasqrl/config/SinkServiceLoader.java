/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.util.ServiceLoaderDiscovery;

import java.util.Optional;

public class SinkServiceLoader {

  public SinkFactory load(String engineName, String sinkType) {
    return ServiceLoaderDiscovery.get(SinkFactory.class, SinkFactory::getEngine, engineName,
            SinkFactory::getSinkType, sinkType);
  }
}
