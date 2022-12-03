package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.spi.JacksonDeserializer;
import lombok.NonNull;

public interface DataSystemDiscoveryConfig extends DataSystemSerializableConfig {

    DataSystemDiscovery initialize(@NonNull ErrorCollector errors);

    class Deserializer extends JacksonDeserializer<DataSystemDiscoveryConfig> {

        public Deserializer() {
            super(DataSystemDiscoveryConfig.class, TYPE_KEY, DataSystemDiscoveryConfig::getSystemType);
        }
    }

}
