package ai.dataeng.sqml.config;

import ai.dataeng.sqml.catalog.ScriptManagerImpl;
import ai.dataeng.sqml.catalog.ScriptProcessorImpl;
import ai.dataeng.sqml.config.provider.CatalogManagerProvider;
import ai.dataeng.sqml.api.graphql.GraphqlSchemaImpl;
import ai.dataeng.sqml.config.provider.DistinctProcessorProvider;
import ai.dataeng.sqml.config.provider.ExpressionProcessorProvider;
import ai.dataeng.sqml.config.provider.GraphqlSchemaProvider;
import ai.dataeng.sqml.api.graphql.GraphqlServletImpl;
import ai.dataeng.sqml.config.provider.GraphqlServletProvider;
import ai.dataeng.sqml.config.provider.HeuristicPlannerProvider;
import ai.dataeng.sqml.config.provider.ImportProcessorProvider;
import ai.dataeng.sqml.config.provider.JoinProcessorProvider;
import ai.dataeng.sqml.config.provider.QueryProcessorProvider;
import ai.dataeng.sqml.config.provider.ScriptManagerProvider;
import ai.dataeng.sqml.config.provider.ScriptProcessorProvider;
import ai.dataeng.sqml.config.provider.SubscriptionProcessorProvider;
import ai.dataeng.sqml.importer.DatasetManagerImpl;
import ai.dataeng.sqml.config.provider.ImportManagerProvider;
import ai.dataeng.sqml.execution.StreamExecutorImpl;
import ai.dataeng.sqml.parser.processor.DistinctProcessor;
import ai.dataeng.sqml.parser.processor.ExpressionProcessor;
import ai.dataeng.sqml.parser.processor.ImportProcessor;
import ai.dataeng.sqml.parser.processor.JoinProcessor;
import ai.dataeng.sqml.parser.processor.QueryProcessor;
import ai.dataeng.sqml.parser.processor.SubscriptionProcessor;
import ai.dataeng.sqml.planner.HeuristicPlannerImpl;
import ai.dataeng.sqml.planner.optimize.OptimizerImpl;
import ai.dataeng.sqml.planner.optimize.OptimizerProvider;
import ai.dataeng.sqml.config.provider.QueryParserProvider;
import ai.dataeng.sqml.parser.ScriptParserImpl;
import ai.dataeng.sqml.config.provider.ScriptParserProvider;
import ai.dataeng.sqml.config.provider.PipelineExecutorProvider;
import ai.dataeng.sqml.parser.QueryParserImpl;
import ai.dataeng.sqml.planner.QueryPlannerImpl;
import ai.dataeng.sqml.config.provider.QueryPlannerProvider;
import ai.dataeng.sqml.catalog.SchemaManagerImpl;
import ai.dataeng.sqml.config.provider.SchemaManagerProvider;
import ai.dataeng.sqml.config.provider.ValidatorProvider;
import ai.dataeng.sqml.parser.validator.ScriptValidatorImpl;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class EnvironmentSettings {
  ValidatorProvider validatorProvider;
  ScriptParserProvider scriptParserProvider;
  QueryParserProvider queryParserProvider;
  QueryPlannerProvider queryPlannerProvider;
  CatalogManagerProvider catalogManagerProvider;
  SchemaManagerProvider schemaManagerProvider;
  ImportManagerProvider importManagerProvider;
  OptimizerProvider optimizerProvider;
  PipelineExecutorProvider pipelineExecutorProvider;
  GraphqlSchemaProvider graphqlSchemaProvider;
  GraphqlServletProvider graphqlServletProvider;
  ScriptProcessorProvider scriptProcessorProvider;
  ScriptManagerProvider scriptManagerProvider;
  HeuristicPlannerProvider heuristicPlannerProvider;
  ImportProcessorProvider importProcessorProvider;
  QueryProcessorProvider queryProcessorProvider;
  ExpressionProcessorProvider expressionProcessorProvider;
  JoinProcessorProvider joinProcessorProvider;
  DistinctProcessorProvider distinctProcessorProvider;
  SubscriptionProcessorProvider subscriptionProcessorProvider;

  public static EnvironmentSettingsBuilder createDefault() {
    return EnvironmentSettings.builder()
        .importProcessorProvider((datasetManager)->new ImportProcessor())
        .queryProcessorProvider(()->new QueryProcessor())
        .expressionProcessorProvider(()->new ExpressionProcessor())
        .joinProcessorProvider(()->new JoinProcessor())
        .distinctProcessorProvider(()->new DistinctProcessor())
        .subscriptionProcessorProvider(()->new SubscriptionProcessor())
        .validatorProvider(()->new ScriptValidatorImpl())
        .scriptParserProvider(()->new ScriptParserImpl())
        .queryParserProvider(()->new QueryParserImpl())
        .queryPlannerProvider(()->new QueryPlannerImpl())
        .schemaManagerProvider(()->new SchemaManagerImpl())
        .importManagerProvider(()->new DatasetManagerImpl())
        .optimizerProvider(()->new OptimizerImpl())
        .pipelineExecutorProvider(()->new StreamExecutorImpl())
        .graphqlSchemaProvider(()->new GraphqlSchemaImpl())
        .graphqlServletProvider(()->new GraphqlServletImpl())
        .heuristicPlannerProvider(()->new HeuristicPlannerImpl())
        .scriptProcessorProvider((importProcessor, queryProcessor, expressionProcessor,
            joinProcessor, distinctProcessor, subscriptionProcessor)->
            new ScriptProcessorImpl(importProcessor, queryProcessor, expressionProcessor,
                joinProcessor, distinctProcessor, subscriptionProcessor))
        .scriptManagerProvider(()->new ScriptManagerImpl());
  }
}
