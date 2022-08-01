package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.config.EnvironmentConfiguration.MetaData;
import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import ai.datasqrl.plan.queries.TableQuery;
import ai.datasqrl.environment.ImportManager;
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
import org.apache.flink.table.planner.delegation.StreamPlanner;

@AllArgsConstructor
public class StreamGraphBuilder {

  private final StreamEngine streamEngine;
  private final ImportManager importManager;
  private final JDBCConnectionProvider jdbcConfiguration;

  public CreateStreamJobResult createStreamGraph(List<TableQuery> streamQueries) {
    final FlinkStreamEngine.Builder streamBuilder = (FlinkStreamEngine.Builder) streamEngine.createJob();
    final StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)streamBuilder.getTableEnvironment();
    final DataStreamRegisterer dataStreamRegisterer = new DataStreamRegisterer(tEnv,
        this.importManager, streamBuilder);

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    List<TableDescriptor> createdTables = new ArrayList<>();
    for (TableQuery sink : streamQueries) {
      String name = sink.getTable().getNameId() + "_sink";
      if (List.of(tEnv.listTables()).contains(name)) {
        continue;
      }
      dataStreamRegisterer.register(sink.getRelNode());

      RelNode relNode = InjectFlinkCluster.injectFlinkRelOptCluster(tEnv, ((StreamPlanner) tEnv.getPlanner()).getRelBuilder().getCluster(),
          sink.getRelNode());

      Table tbl = FlinkEnvProxy.relNodeQuery(relNode, tEnv);

      TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
          .schema(FlinkPipelineUtils.addPrimaryKey(tbl.getSchema().toSchema(), sink.getTable()))
          .option("url", jdbcConfiguration.getDbURL())
          .option("table-name", sink.getTable().getNameId())
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
