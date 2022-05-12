package ai.datasqrl.physical.stream;

import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine;
import ai.datasqrl.physical.stream.rel.InjectFlinkCluster;
import ai.datasqrl.plan.queries.TableQuery;
import ai.datasqrl.server.ImportManager;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.api.internal.FlinkEnvProxy;

@AllArgsConstructor
public class StreamGraphBuilder {

  private final StreamEngine streamEngine;
  private final ImportManager importManager;
  private final JDBCConfiguration jdbcConfiguration;

  public CreateStreamJobResult createStreamGraph(List<TableQuery> streamQueries) {
    final FlinkStreamEngine.Builder streamBuilder = (FlinkStreamEngine.Builder) streamEngine.createStream();
    final StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)
        StreamTableEnvironment.create(streamBuilder.getEnvironment());
    final DataStreamRegisterer dataStreamRegisterer = new DataStreamRegisterer(tEnv,
        this.importManager, streamBuilder);

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    List<TableDescriptor> createdTables = new ArrayList<>();
    for (TableQuery sink : streamQueries) {
      String name = sink.getTable().getName().getCanonical() + "_sink";
      if (List.of(tEnv.listTables()).contains(name)) {
        continue;
      }
      dataStreamRegisterer.register(sink.getRelNode());

      RelNode relNode = InjectFlinkCluster.injectFlinkRelOptCluster(tEnv,
          sink.getRelNode().getInput(0));

      Table tbl = FlinkEnvProxy.relNodeQuery(relNode, tEnv);

      TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
          .schema(FlinkPipelineUtils.addPrimaryKey(tbl.getSchema().toSchema(), sink.getTable()))
          .option("url", jdbcConfiguration.getDbURL().concat("/").concat(MetaData.DEFAULT_DATABASE))
          .option("table-name", sink.getTable().getName().getCanonical())
          .option("username", jdbcConfiguration.getUser())
          .option("password", jdbcConfiguration.getPassword())
          .build();

      tEnv.createTable(name, descriptor);

      stmtSet.addInsert(name, tbl);
      createdTables.add(descriptor);
    }

    return new CreateStreamJobResult(stmtSet, createdTables);
  }
}
