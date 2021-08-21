package ai.dataeng.sqml.metadata;

import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.query.GraphqlQueryProvider;
import ai.dataeng.sqml.registry.ScriptRegistry;
import ai.dataeng.sqml.schema.SchemaProvider;
import ai.dataeng.sqml.statistics.StatisticsProvider;
import ai.dataeng.sqml.tree.Script;

public class Metadata {

  private final FunctionProvider functionProvider;
  private final ScriptRegistry scriptRegistry;
  private final GraphqlQueryProvider queries;
  private final StatisticsProvider statisticsProvider;
  private final Bundle bundle;
  private final SchemaProvider schemaProvider;

  public Metadata(FunctionProvider functionProvider, ScriptRegistry scriptRegistry,
      GraphqlQueryProvider queries, StatisticsProvider statisticsProvider,
      Bundle bundle, SchemaProvider schemaProvider) {

    this.functionProvider = functionProvider;
    this.scriptRegistry = scriptRegistry;
    this.queries = queries;
    this.statisticsProvider = statisticsProvider;
    this.bundle = bundle;
    this.schemaProvider = schemaProvider;
  }

  public FunctionProvider getFunctionProvider() {
    return functionProvider;
  }

  public ScriptRegistry getScriptRegistry() {
    return scriptRegistry;
  }

  public GraphqlQueryProvider getQueries() {
    return queries;
  }

  public StatisticsProvider getStatisticsProvider() {
    return statisticsProvider;
  }

  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  public Script getScript(String name) {
    return scriptRegistry.getScript(name);
  }

  public Bundle getBundle() {
    return this.bundle;
  }
}
