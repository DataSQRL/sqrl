package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import ai.datasqrl.plan.global.OptimizedDAG;
import graphql.com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.table.api.internal.FlinkEnvProxy;

import java.util.List;

@AllArgsConstructor
public class FlinkPhysicalPlanner {

  private final StreamEngine streamEngine;
  private final ImportManager importManager;
  private final JDBCConnectionProvider jdbcConfiguration;

  public FlinkStreamPhysicalPlan createStreamGraph(List<OptimizedDAG.MaterializeQuery> streamQueries) {
    final FlinkStreamEngine.Builder streamBuilder = (FlinkStreamEngine.Builder) streamEngine.createJob();
    final StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)streamBuilder.getTableEnvironment();
    final DataStreamRegisterer dataStreamRegisterer = new DataStreamRegisterer(tEnv,
        this.importManager, streamBuilder);

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    for (OptimizedDAG.MaterializeQuery query : streamQueries) {
      Preconditions.checkArgument(query.getSink() instanceof OptimizedDAG.TableSink, "Subscriptions not yet implemented");
      OptimizedDAG.TableSink tblsink = ((OptimizedDAG.TableSink) query.getSink());

      String name = tblsink.getNameId() + "_sink";
      if (List.of(tEnv.listTables()).contains(name)) {
        continue;
      }
      dataStreamRegisterer.register(query.getRelNode());

      RelNode relNode = FlinkPhysicalPlanRewriter.rewrite(tEnv, query.getRelNode());

      Table tbl = FlinkEnvProxy.relNodeQuery(relNode, tEnv);

      TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
          .schema(FlinkPipelineUtils.addPrimaryKey(tbl.getSchema().toSchema(), tblsink))
          .option("url", jdbcConfiguration.getDbURL())
          .option("table-name", tblsink.getNameId())
          .option("username", jdbcConfiguration.getUser())
          .option("password", jdbcConfiguration.getPassword())
          .build();

      tEnv.createTable(name, descriptor);
      stmtSet.addInsert(name, tbl);
    }

    return new FlinkStreamPhysicalPlan(stmtSet);
  }
}
