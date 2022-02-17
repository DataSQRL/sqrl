package ai.dataeng.sqml.config;

import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.catalog.NamespaceImpl;
import ai.dataeng.sqml.config.engines.FlinkConfiguration;
import ai.dataeng.sqml.config.engines.JDBCConfiguration;
import ai.dataeng.sqml.config.metadata.JDBCMetadataStore;
import ai.dataeng.sqml.config.provider.*;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.ingest.FlinkSourceMonitor;
import ai.dataeng.sqml.execution.flink.process.FlinkGenerator;
import ai.dataeng.sqml.execution.sql.SQLGenerator;
import ai.dataeng.sqml.io.sources.dataset.MetadataRegistryPersistence;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitorImpl;
import ai.dataeng.sqml.planner.operator.ImportManager;
import ai.dataeng.sqml.parser.ScriptParserImpl;
import ai.dataeng.sqml.parser.processor.DistinctProcessorImpl;
import ai.dataeng.sqml.parser.processor.ExpressionProcessorImpl;
import ai.dataeng.sqml.parser.processor.ImportProcessorImpl;
import ai.dataeng.sqml.parser.processor.JoinProcessorImpl;
import ai.dataeng.sqml.parser.processor.QueryProcessorImpl;
import ai.dataeng.sqml.parser.processor.ScriptProcessorImpl;
import ai.dataeng.sqml.parser.processor.SubscriptionProcessorImpl;
import ai.dataeng.sqml.parser.validator.ScriptValidatorImpl;
import ai.dataeng.sqml.planner.HeuristicPlannerImpl;
import ai.dataeng.sqml.planner.operator.ImportResolver;
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
  ImportProcessorProvider importProcessorProvider;
  QueryProcessorProvider queryProcessorProvider;
  ExpressionProcessorProvider expressionProcessorProvider;
  JoinProcessorProvider joinProcessorProvider;
  DistinctProcessorProvider distinctProcessorProvider;
  SubscriptionProcessorProvider subscriptionProcessorProvider;

  JDBCConfiguration jdbcConfiguration;
  StreamEngineProvider streamEngineProvider;

  SqlGeneratorProvider sqlGeneratorProvider;
  StreamMonitorProvider streamMonitorProvider;
  StreamGeneratorProvider streamGeneratorProvider;

  EnvironmentConfiguration environmentConfiguration;
  MetadataStoreProvider metadataStoreProvider;
  DatasetRegistryPersistenceProvider datasetRegistryPersistenceProvider;
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

        .namespace(new NamespaceImpl())
        .importProcessorProvider((importResolver, planner)->new ImportProcessorImpl(importResolver, planner))
        .queryProcessorProvider((planner)->new QueryProcessorImpl(planner))
        .expressionProcessorProvider((planner)->new ExpressionProcessorImpl(planner))
        .joinProcessorProvider(()->new JoinProcessorImpl())
        .distinctProcessorProvider(()->new DistinctProcessorImpl())
        .subscriptionProcessorProvider(()->new SubscriptionProcessorImpl())
        .validatorProvider(()->new ScriptValidatorImpl())
        .scriptParserProvider(()->new ScriptParserImpl())
        .importManagerProvider((datasetLookup)-> {
          ImportManager manager = new ImportManager(datasetLookup);
          return new ImportResolver(manager);
        })
        .heuristicPlannerProvider(()->new HeuristicPlannerImpl())
        .scriptProcessorProvider((importProcessor, queryProcessor, expressionProcessor,
            joinProcessor, distinctProcessor, subscriptionProcessor, namespace)->
            new ScriptProcessorImpl(importProcessor, queryProcessor, expressionProcessor,
                joinProcessor, distinctProcessor, subscriptionProcessor, namespace));

    GlobalConfiguration.Engines engines = config.getEngines();
    FlinkConfiguration flinkConfig = engines.getFlink();
    builder.streamEngineProvider(flinkConfig);
    builder.streamGeneratorProvider((flink, jdbc) -> new FlinkGenerator(jdbc, (FlinkStreamEngine) flink));
    builder.streamMonitorProvider((flink, jdbc, meta, registry) ->
            new FlinkSourceMonitor((FlinkStreamEngine) flink,jdbc,meta, registry));

    if (!config.getEnvironment().isMonitor_sources()) {
      builder.sourceTableMonitorProvider(SourceTableMonitorProvider.NO_MONITORING);
    } else {
      builder.sourceTableMonitorProvider((engine,sourceMonitor) -> new SourceTableMonitorImpl(engine,sourceMonitor));
    }

    return builder;
  }
}
