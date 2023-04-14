/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.serializer.JacksonDeserializer;
import com.google.auto.service.AutoService;
import lombok.NonNull;

public interface DataSystemDiscoveryConfig extends DataSystemSerializableConfig {

  DataSystemDiscovery initialize(@NonNull ErrorCollector errors);

  @AutoService(JacksonDeserializer.class)
  class Deserializer extends JacksonDeserializer<DataSystemDiscoveryConfig> {

    public Deserializer() {
      super(DataSystemDiscoveryConfig.class, TYPE_KEY, DataSystemDiscoveryConfig::getSystemType);
    }
  }

}
