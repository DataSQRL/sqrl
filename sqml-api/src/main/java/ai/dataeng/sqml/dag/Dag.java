package ai.dataeng.sqml.dag;

import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.optimizer.Optimizer;
import ai.dataeng.sqml.optimizer.OptimizerResult;
import ai.dataeng.sqml.query.GraphqlQueryProvider;
import ai.dataeng.sqml.registry.ScriptRegistry;
import ai.dataeng.sqml.schema.SchemaProvider;
import ai.dataeng.sqml.statistics.StatisticsProvider;

public class Dag {

  private final OptimizerResult result;
  private final GraphqlQueryProvider queries;

  public Dag(OptimizerResult result,
      GraphqlQueryProvider queries) {
    this.result = result;
    this.queries = queries;
  }

  public GraphqlQueryProvider getQueries() {
    return queries;
  }

  public OptimizerResult getOptimizationResult() {
    return result;
  }

  public static Builder newDag() {
    return new Builder();
  }

  public static class Builder {

    private FunctionProvider functionProvider;
    private ScriptRegistry scriptRegistry;
    private GraphqlQueryProvider queries;
    private StatisticsProvider statisticsProvider;
    private Optimizer optimizer;
    private SchemaProvider schemaProvider;
    private Bundle bundle;

    public Builder bundle(Bundle bundle) {
      this.bundle = bundle;
      return this;
    }

    public Builder functionProvider(FunctionProvider functionProvider) {
      this.functionProvider = functionProvider;
      return this;
    }
    public Builder functionProvider(FunctionProvider.Builder functionProvider) {
      this.functionProvider = functionProvider.build();
      return this;
    }

    public Builder scriptRegistry(ScriptRegistry scriptRegistry) {
      this.scriptRegistry = scriptRegistry;
      return this;
    }
    public Builder scriptRegistry(ScriptRegistry.Builder scriptRegistry) {
      this.scriptRegistry = scriptRegistry.build();
      return this;
    }

    public Builder queryProvider(GraphqlQueryProvider queries) {
      this.queries = queries;
      return this;
    }

    public Builder queryProvider(GraphqlQueryProvider.Builder queries) {
      this.queries = queries.build();
      return this;
    }

    public Builder statisticsProvider(StatisticsProvider statisticsProvider) {
      this.statisticsProvider = statisticsProvider;
      return this;
    }
    public Builder statisticsProvider(StatisticsProvider.Builder statisticsProvider) {
      this.statisticsProvider = statisticsProvider.build();
      return this;
    }

    public Builder schemaProvider(SchemaProvider.Builder schemaProvider) {
      this.schemaProvider = schemaProvider.build();
      return this;
    }

    public Builder schemaProvider(SchemaProvider schemaProvider) {
      this.schemaProvider = schemaProvider;
      return this;
    }

    public Builder optimizer(Optimizer optimizer) {
      this.optimizer = optimizer;
      return this;
    }

    public Builder optimizer(Optimizer.Builder optimizer) {
      this.optimizer = optimizer.build();
      return this;
    }

    public Dag build(String name) {
      OptimizerResult result = optimizer.optimize(name, new Metadata(functionProvider, scriptRegistry,
          queries, statisticsProvider, bundle, schemaProvider));
      return new Dag(result, queries);
    }
  }
}
