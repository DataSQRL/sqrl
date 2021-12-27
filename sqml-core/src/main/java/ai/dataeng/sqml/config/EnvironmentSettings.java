package ai.dataeng.sqml.config;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.catalog.NamespaceImpl;
import ai.dataeng.sqml.catalog.persistence.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.catalog.persistence.keyvalue.LocalFileHierarchyKeyValueStore;
import ai.dataeng.sqml.config.provider.DistinctProcessorProvider;
import ai.dataeng.sqml.config.provider.ExpressionProcessorProvider;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.ImportManagerProvider;
import ai.dataeng.sqml.config.provider.ImportProcessorProvider;
import ai.dataeng.sqml.config.provider.JoinProcessorProvider;
import ai.dataeng.sqml.config.provider.QueryProcessorProvider;
import ai.dataeng.sqml.config.provider.ScriptParserProvider;
import ai.dataeng.sqml.config.provider.ScriptProcessorProvider;
import ai.dataeng.sqml.config.provider.SubscriptionProcessorProvider;
import ai.dataeng.sqml.config.provider.ValidatorProvider;
import ai.dataeng.sqml.execution.flink.ingest.DataSourceRegistry;
import ai.dataeng.sqml.importer.ImportManager;
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
import java.nio.file.Path;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class EnvironmentSettings {
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
  Namespace namespace;
  DataSourceRegistry dsLookup;

  public static EnvironmentSettingsBuilder createDefault() {
    Path outputBase = Path.of("tmp","datasource");

    HierarchyKeyValueStore.Factory kvStoreFactory = new LocalFileHierarchyKeyValueStore.Factory(outputBase.toString());
    DataSourceRegistry dsLookup = new DataSourceRegistry(kvStoreFactory);

    return EnvironmentSettings.builder()
        .namespace(new NamespaceImpl())
        .importProcessorProvider((importResolver, planner)->new ImportProcessorImpl(importResolver, planner))
        .queryProcessorProvider(()->new QueryProcessorImpl())
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
                joinProcessor, distinctProcessor, subscriptionProcessor, namespace))
        .dsLookup(dsLookup);
  }
}
