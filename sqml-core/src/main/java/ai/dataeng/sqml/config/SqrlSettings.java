package ai.dataeng.sqml.config;

import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.sqml.MetadataEnvironmentPersistence;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.catalog.NamespaceImpl;
import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.metadata.JDBCMetadataStore;
import ai.dataeng.sqml.config.provider.DatasetRegistryPersistenceProvider;
import ai.dataeng.sqml.config.provider.EnvironmentPersistenceProvider;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.ImportManagerProvider;
import ai.dataeng.sqml.config.provider.MetadataStoreProvider;
import ai.dataeng.sqml.config.provider.ScriptParserProvider;
import ai.dataeng.sqml.config.provider.ScriptProcessorProvider;
import ai.dataeng.sqml.config.provider.SourceTableMonitorProvider;
import ai.dataeng.sqml.config.provider.SqlGeneratorProvider;
import ai.dataeng.sqml.config.provider.StreamEngineProvider;
import ai.dataeng.sqml.config.provider.StreamGeneratorProvider;
import ai.dataeng.sqml.config.provider.StreamMonitorProvider;
import ai.dataeng.sqml.config.provider.ValidatorProvider;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.ingest.FlinkSourceMonitor;
import ai.dataeng.sqml.execution.flink.process.FlinkGenerator;
import ai.dataeng.sqml.execution.sql.SQLGenerator;
import ai.dataeng.sqml.io.sources.dataset.MetadataRegistryPersistence;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitorImpl;
import ai.dataeng.sqml.parser.ScriptParserImpl;
import ai.dataeng.sqml.parser.processor.ScriptProcessorImpl;
import ai.dataeng.sqml.parser.validator.ScriptValidatorImpl;
import ai.dataeng.sqml.planner.HeuristicPlannerImpl;
import ai.dataeng.sqml.planner.operator.ImportManager;
import ai.dataeng.sqml.planner.operator.ImportResolver;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SqrlSettings {
  Namespace namespace;
  ValidatorProvider validatorProvider;
  ScriptParserProvider scriptParserProvider;
  ImportManagerProvider importManagerProvider;
  ScriptProcessorProvider scriptProcessorProvider;
  HeuristicPlannerProvider heuristicPlannerProvider;

  JDBCConfiguration jdbcConfiguration;
  StreamEngineProvider streamEngineProvider;

  SqlGeneratorProvider sqlGeneratorProvider;
  StreamMonitorProvider streamMonitorProvider;
  StreamGeneratorProvider streamGeneratorProvider;

  EnvironmentConfiguration environmentConfiguration;
  MetadataStoreProvider metadataStoreProvider;
  DatasetRegistryPersistenceProvider datasetRegistryPersistenceProvider;
  EnvironmentPersistenceProvider environmentPersistenceProvider;
  SourceTableMonitorProvider sourceTableMonitorProvider;

  SqlClientProvider sqlClientProvider;


  public static SqrlSettings fromConfiguration(GlobalConfiguration config) {
    return builderFromConfiguration(config).build();
  }

  public static SqrlSettingsBuilder builderFromConfiguration(GlobalConfiguration config) {
    SqrlSettingsBuilder builder =  SqrlSettings.builder()
        .jdbcConfiguration(config.getEngines().getJdbc())
        .sqlGeneratorProvider(jdbc->new SQLGenerator(jdbc))

        .environmentConfiguration(config.getEnvironment())
        .metadataStoreProvider(new JDBCMetadataStore.Provider())
        .datasetRegistryPersistenceProvider(new MetadataRegistryPersistence.Provider())
        .environmentPersistenceProvider(new MetadataEnvironmentPersistence.Provider())

        .namespace(new NamespaceImpl())
        .validatorProvider(()->new ScriptValidatorImpl())
        .scriptParserProvider(()->new ScriptParserImpl())
        .importManagerProvider((datasetLookup, planner)-> {
          ImportManager manager = new ImportManager(datasetLookup);
          return new ImportResolver(manager, planner);
        })
        .heuristicPlannerProvider(()->new HeuristicPlannerImpl())
        .scriptProcessorProvider(ScriptProcessorImpl::new);

    GlobalConfiguration.Engines engines = config.getEngines();
    Preconditions.checkArgument(engines.getFlink()!=null,"Must configure Flink engine");
    FlinkConfiguration flinkConfig = engines.getFlink();
    builder.streamEngineProvider(flinkConfig);
    builder.streamGeneratorProvider((flink, jdbc) -> new FlinkGenerator(jdbc, (FlinkStreamEngine) flink));
    builder.streamMonitorProvider((flink, jdbc, meta, registry) ->
            new FlinkSourceMonitor((FlinkStreamEngine) flink,jdbc,meta, registry));

    if (!config.getEnvironment().isMonitorSources()) {
      builder.sourceTableMonitorProvider(SourceTableMonitorProvider.NO_MONITORING);
    } else {
      builder.sourceTableMonitorProvider((engine,sourceMonitor) -> new SourceTableMonitorImpl(engine,sourceMonitor));
    }

    return builder;
  }
}
