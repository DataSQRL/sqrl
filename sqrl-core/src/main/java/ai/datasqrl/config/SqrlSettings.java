package ai.datasqrl.config;

import ai.datasqrl.graphql.execution.SqlClientProvider;
import ai.datasqrl.config.provider.DatasetRegistryPersistenceProvider;
import ai.datasqrl.config.provider.ImportManagerProvider;
import ai.datasqrl.config.provider.StreamEngineProvider;
import ai.datasqrl.config.provider.StreamMonitorProvider;
import ai.datasqrl.server.MetadataEnvironmentPersistence;
import ai.datasqrl.config.engines.FlinkConfiguration;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.serializer.KryoProvider;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine;
import ai.datasqrl.execute.flink.ingest.FlinkSourceMonitor;
import ai.datasqrl.io.sinks.registry.MetadataSinkRegistryPersistence;
import ai.datasqrl.io.sources.dataset.MetadataSourceRegistryPersistence;
import ai.datasqrl.io.sources.dataset.SourceTableMonitorImpl;
import ai.datasqrl.server.ImportManager;
import ai.datasqrl.config.metadata.JDBCMetadataStore.Provider;
import ai.datasqrl.config.provider.DataSinkRegistryPersistenceProvider;
import ai.datasqrl.config.provider.EnvironmentPersistenceProvider;
import ai.datasqrl.config.provider.MetadataStoreProvider;
import ai.datasqrl.config.provider.SerializerProvider;
import ai.datasqrl.config.provider.SourceTableMonitorProvider;
import ai.datasqrl.config.provider.SqlGeneratorProvider;
import ai.datasqrl.config.provider.StreamGeneratorProvider;
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
  StreamMonitorProvider streamMonitorProvider;
  StreamGeneratorProvider streamGeneratorProvider;

  EnvironmentConfiguration environmentConfiguration;
  MetadataStoreProvider metadataStoreProvider;
  SerializerProvider serializerProvider;
  DatasetRegistryPersistenceProvider datasetRegistryPersistenceProvider;
  DataSinkRegistryPersistenceProvider dataSinkRegistryPersistenceProvider;
  EnvironmentPersistenceProvider environmentPersistenceProvider;
  SourceTableMonitorProvider sourceTableMonitorProvider;

  SqlClientProvider sqlClientProvider;


  public static SqrlSettings fromConfiguration(GlobalConfiguration config) {
    return builderFromConfiguration(config).build();
  }

  public static SqrlSettingsBuilder builderFromConfiguration(GlobalConfiguration config) {
    SqrlSettingsBuilder builder =  SqrlSettings.builder()
        .jdbcConfiguration(config.getEngines().getJdbc())
//        .sqlGeneratorProvider(jdbc->new SQLGenerator(jdbc))

        .environmentConfiguration(config.getEnvironment())
        .serializerProvider(new KryoProvider())
        .metadataStoreProvider(new Provider())
        .datasetRegistryPersistenceProvider(new MetadataSourceRegistryPersistence.Provider())
        .dataSinkRegistryPersistenceProvider(new MetadataSinkRegistryPersistence.Provider())
        .environmentPersistenceProvider(new MetadataEnvironmentPersistence.Provider())
        .importManagerProvider((datasetLookup)-> {
          ImportManager manager = new ImportManager(datasetLookup);
          return manager;
        });

    GlobalConfiguration.Engines engines = config.getEngines();
    Preconditions.checkArgument(engines.getFlink()!=null,"Must configure Flink engine");
    FlinkConfiguration flinkConfig = engines.getFlink();
    builder.streamEngineProvider(flinkConfig);
//    builder.streamGeneratorProvider((flink, jdbc) -> new FlinkGenerator(jdbc, (FlinkStreamEngine) flink));
    builder.streamMonitorProvider((flink, jdbc, meta, serializer, registry) ->
            new FlinkSourceMonitor((FlinkStreamEngine) flink,jdbc,meta, serializer, registry));

    if (!config.getEnvironment().isMonitorSources()) {
      builder.sourceTableMonitorProvider(SourceTableMonitorProvider.NO_MONITORING);
    } else {
      builder.sourceTableMonitorProvider((engine,sourceMonitor) -> new SourceTableMonitorImpl(engine,sourceMonitor));
    }

    return builder;
  }
}
