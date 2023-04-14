package com.datasqrl.io;

import com.datasqrl.serializer.JacksonDeserializer;
import com.google.auto.service.AutoService;

@AutoService(JacksonDeserializer.class)
public class DataSystemConnectorDeserializer extends JacksonDeserializer<DataSystemConnectorConfig> {

  public DataSystemConnectorDeserializer() {
    super(DataSystemConnectorConfig.class, DataSystemSerializableConfig.TYPE_KEY,
        DataSystemConnectorConfig::getSystemType);
  }
}
