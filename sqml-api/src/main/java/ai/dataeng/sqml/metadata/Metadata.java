package ai.dataeng.sqml.metadata;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.execution.Bundle;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.ingest.DataSourceRegistry;
import ai.dataeng.sqml.ingest.stats.SourceTableStatistics;
import ai.dataeng.sqml.query.GraphqlQueryProvider;
import ai.dataeng.sqml.registry.ScriptRegistry;
import ai.dataeng.sqml.schema.SchemaProvider;
import ai.dataeng.sqml.ingest.source.SourceDataset;
import ai.dataeng.sqml.ingest.source.SourceTable;
import ai.dataeng.sqml.statistics.StatisticsProvider;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import lombok.Value;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Metadata {

  private final FunctionProvider functionProvider;
  private final ScriptRegistry scriptRegistry;
  private final GraphqlQueryProvider queries;
  private final StatisticsProvider statisticsProvider;
  private final Bundle bundle;
  private final SchemaProvider schemaProvider;
  private final DataSourceRegistry datasetRegistry;
  private final StreamExecutionEnvironment flinkEnv;
  private final StreamTableEnvironment streamTableEnvironment;

  private Map<QualifiedName, TableHandle> tableHandles = new HashMap<>();

  public Metadata(FunctionProvider functionProvider, ScriptRegistry scriptRegistry,
      GraphqlQueryProvider queries, StatisticsProvider statisticsProvider,
      Bundle bundle, SchemaProvider schemaProvider,
      DataSourceRegistry datasetRegistry,
      StreamExecutionEnvironment flinkEnv,
      StreamTableEnvironment streamTableEnvironment) {

    this.functionProvider = functionProvider;
    this.scriptRegistry = scriptRegistry;
    this.queries = queries;
    this.statisticsProvider = statisticsProvider;
    this.bundle = bundle;
    this.schemaProvider = schemaProvider;
    this.datasetRegistry = datasetRegistry;
    this.flinkEnv = flinkEnv;
    this.streamTableEnvironment = streamTableEnvironment;
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

  public SourceTableStatistics getDatasourceTableStatistics(SourceTable stable) {
    Preconditions.checkNotNull(stable, "Source table cannot be null");
    return datasetRegistry.getTableStatistics(stable);
  }

  public StreamExecutionEnvironment getFlinkEnv() {
    return flinkEnv;
  }

  public StreamTableEnvironment getStreamTableEnvironment() {
    return streamTableEnvironment;
  }

  public void registerTable(QualifiedName path, Table table,
      DestinationTableSchema resultSchema) {
    tableHandles.put(path, new TableHandle(table, resultSchema));
  }

  public SourceDataset getDataset(String name) {
    return datasetRegistry.getDataset(name);
  }

  public Map<QualifiedName, TableHandle> getTableHandles() {
    return tableHandles;
  }

  public TableHandle getTableHandle(QualifiedName name) {
    TableHandle tableHandle = tableHandles.get(name);
    Preconditions.checkNotNull(tableHandle, "Table handle cannot be null. Looking for %s", name);
    return tableHandle;
  }

  @Value
  public static class TableHandle {
    Table table;
    DestinationTableSchema schema;
  }
}
