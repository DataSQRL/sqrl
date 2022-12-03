package ai.datasqrl.io.sources;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.spi.JacksonDeserializer;
import lombok.NonNull;

/**
 * The configuration of a data source that DataSQRL can connect to for data access
 */
public interface DataSystemConnectorConfig extends DataSystemSerializableConfig {

  DataSystemConnector initialize(@NonNull ErrorCollector errors);

  class Deserializer extends JacksonDeserializer<DataSystemConnectorConfig> {

    public Deserializer() {
      super(DataSystemConnectorConfig.class, TYPE_KEY, DataSystemConnectorConfig::getSystemType);
    }
  }

}
