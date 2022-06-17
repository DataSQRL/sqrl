package ai.datasqrl.config;

import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.InMemoryStreamConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.metadata.JDBCMetadataStore.Provider;
import ai.datasqrl.config.provider.*;
import ai.datasqrl.config.serializer.KryoProvider;
import ai.datasqrl.graphql.execution.SqlClientProvider;
import ai.datasqrl.io.sinks.registry.MetadataSinkRegistryPersistence;
import ai.datasqrl.io.sources.dataset.MetadataSourceRegistryPersistence;
import ai.datasqrl.io.sources.dataset.SourceTableMonitorImpl;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.MetadataEnvironmentPersistence;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SqrlSettings {

  ImportManagerProvider importManagerProvider;

  JDBCConfiguration jdbcConfiguration;
  StreamEngineProvider streamEngineProvider;

  SqlGeneratorProvider sqlGeneratorProvider;

  EnvironmentConfiguration environmentConfiguration;
  MetadataStoreProvider metadataStoreProvider;
  SerializerProvider serializerProvider;
  DatasetRegistryPersistenceProvider datasetRegistryPersistenceProvider;
  DataSinkRegistryPersistenceProvider dataSinkRegistryPersistenceProvider;
  EnvironmentPersistenceProvider environmentPersistenceProvider;
  SourceTableMonitorProvider sourceTableMonitorProvider;
  TableStatisticsStoreProvider tableStatisticsStoreProvider;

  SqlClientProvider sqlClientProvider;

  public static SqrlSettings fromConfiguration(GlobalConfiguration config) {
    return builderFromConfiguration(config).build();
  }

  public static SqrlSettingsBuilder builderFromConfiguration(GlobalConfiguration config) {
    SqrlSettingsBuilder builder = SqrlSettings.builder()
        .jdbcConfiguration(config.getEngines().getJdbc())
        .environmentConfiguration(config.getEnvironment())
        .serializerProvider(new KryoProvider())
        .metadataStoreProvider(new Provider())
        .datasetRegistryPersistenceProvider(new MetadataSourceRegistryPersistence.RegistryProvider())
        .dataSinkRegistryPersistenceProvider(new MetadataSinkRegistryPersistence.Provider())
        .environmentPersistenceProvider(new MetadataEnvironmentPersistence.Provider())
        .importManagerProvider(ImportManager::new);

    GlobalConfiguration.Engines engines = config.getEngines();
    if (engines.getFlink() != null) {
      builder.streamEngineProvider(engines.getFlink());
    } else if (engines.getInmemory() != null) {
      builder.streamEngineProvider(engines.getInmemory());

    } else throw new IllegalArgumentException("Must configure a stream engine");
    builder.tableStatisticsStoreProvider(new MetadataSourceRegistryPersistence.TableStatsProvider());


    if (!config.getEnvironment().isMonitorSources()) {
      builder.sourceTableMonitorProvider(SourceTableMonitorProvider.NO_MONITORING);
    } else {
      builder.sourceTableMonitorProvider(
          (engine, statsStore) -> new SourceTableMonitorImpl(engine, statsStore, new StreamInputPreparerImpl()));
    }

    return builder;
  }
}
