package com.datasqrl.io;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.spi.JacksonDeserializer;
import lombok.NonNull;

public interface DataSystemDiscoveryConfig extends DataSystemSerializableConfig {

  DataSystemDiscovery initialize(@NonNull ErrorCollector errors);

  class Deserializer extends JacksonDeserializer<DataSystemDiscoveryConfig> {

    public Deserializer() {
      super(DataSystemDiscoveryConfig.class, TYPE_KEY, DataSystemDiscoveryConfig::getSystemType);
    }
  }

}
