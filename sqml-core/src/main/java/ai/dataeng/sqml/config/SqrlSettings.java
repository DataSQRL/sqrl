package ai.dataeng.sqml.config;

import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.sqml.MetadataEnvironmentPersistence;
import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.metadata.JDBCMetadataStore;
import ai.dataeng.sqml.config.provider.*;
import ai.dataeng.sqml.config.serializer.KryoProvider;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.ingest.FlinkSourceMonitor;
import ai.dataeng.sqml.io.sinks.registry.MetadataSinkRegistryPersistence;
import ai.dataeng.sqml.io.sources.dataset.MetadataSourceRegistryPersistence;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitorImpl;
import ai.dataeng.sqml.parser.operator.ImportManager;
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
        .metadataStoreProvider(new JDBCMetadataStore.Provider())
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
