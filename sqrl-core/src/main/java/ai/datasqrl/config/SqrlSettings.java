package ai.datasqrl.config;

import ai.datasqrl.config.metadata.FileMetadataStore;
import ai.datasqrl.config.metadata.InMemoryMetadataStore;
import ai.datasqrl.config.metadata.JDBCMetadataStore;
import ai.datasqrl.config.provider.*;
import ai.datasqrl.config.serializer.KryoProvider;
import ai.datasqrl.config.metadata.MetadataNamedPersistence;
import ai.datasqrl.compile.SourceTableMonitorImpl;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
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
  SourceTableMonitorProvider sourceTableMonitorProvider;
  TableStatisticsStoreProvider tableStatisticsStoreProvider;

  public static SqrlSettings fromConfiguration(GlobalConfiguration config) {
    return builderFromConfiguration(config).build();
  }

  public static SqrlSettingsBuilder builderFromConfiguration(GlobalConfiguration config) {
    SqrlSettingsBuilder builder = SqrlSettings.builder()
        .discoveryConfiguration(config.getDiscovery())
        .serializerProvider(new KryoProvider());

    GlobalConfiguration.Engines engines = config.getEngines();
    if (engines.getFlink() != null) {
      builder.streamEngineProvider(engines.getFlink());
    } else if (engines.getInmemoryStream() != null) {
      builder.streamEngineProvider(engines.getInmemoryStream());
    } else throw new IllegalArgumentException("Must configure a stream engine");
    builder.tableStatisticsStoreProvider(new MetadataNamedPersistence.TableStatsProvider());

    if (engines.getJdbc() != null) {
      builder.databaseEngineProvider(config.getEngines().getJdbc());
      builder.metadataStoreProvider(new JDBCMetadataStore.Provider());
    } else if (engines.getInmemoryDB() != null) {
      builder.databaseEngineProvider(config.getEngines().getInmemoryDB());
      builder.metadataStoreProvider(new InMemoryMetadataStore.Provider());
    } else if (engines.getFileDB() != null) {
      builder.databaseEngineProvider(config.getEngines().getFileDB());
      builder.metadataStoreProvider(new FileMetadataStore.Provider());
    }else throw new IllegalArgumentException("Must configure a database engine");

    if (!config.getDiscovery().isMonitorSources()) {
      builder.sourceTableMonitorProvider(SourceTableMonitorProvider.NO_MONITORING);
    } else {
      builder.sourceTableMonitorProvider(
          (engine, statsStore) -> new SourceTableMonitorImpl(engine, statsStore, new StreamInputPreparerImpl()));
    }

    return builder;
  }
}
