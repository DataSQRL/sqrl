package com.datasqrl.io.sources;

import com.datasqrl.config.error.ErrorCollector;
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
