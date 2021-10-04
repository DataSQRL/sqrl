package ai.dataeng.sqml.metadata;

import ai.dataeng.sqml.db.DestinationTableSchema;
import ai.dataeng.sqml.env.SqmlEnv;
import ai.dataeng.sqml.execution.SQMLBundle;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.imports.ImportLoader;
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

@Value
public class Metadata {

  private final FunctionProvider functionProvider;
  private final SqmlEnv env;
  private final ImportLoader importLoader;
}
