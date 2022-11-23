package ai.datasqrl.config;

import ai.datasqrl.config.provider.*;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SqrlSettings {

  DatabaseEngineProvider databaseEngineProvider;
  StreamEngineProvider streamEngineProvider;

  SqlGeneratorProvider sqlGeneratorProvider;

  DiscoveryConfiguration discoveryConfiguration;
  MetadataStoreProvider metadataStoreProvider;
  SerializerProvider serializerProvider;
}
