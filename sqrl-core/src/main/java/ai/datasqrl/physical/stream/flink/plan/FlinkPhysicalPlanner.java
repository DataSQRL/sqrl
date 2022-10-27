package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.util.db.JDBCTempDatabase;
import graphql.com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;

import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class FlinkPhysicalPlanner {

  private final StreamEngine streamEngine;
  private final ImportManager importManager;
  private final JDBCConnectionProvider jdbcConfiguration;
  private final Optional<JDBCTempDatabase> jdbcTempDatabase;

  public FlinkStreamPhysicalPlan createStreamGraph(List<OptimizedDAG.MaterializeQuery> streamQueries) {
    final FlinkStreamEngine.Builder streamBuilder = (FlinkStreamEngine.Builder) streamEngine.createJob();
    final StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)streamBuilder.getTableEnvironment();
    final TableRegisterer tableRegisterer = new TableRegisterer(tEnv,
        this.importManager, streamBuilder);

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    //TODO: push down filters across queries to determine if we can constraint sources by time for efficiency (i.e. only load the subset of the stream that is required)
    for (OptimizedDAG.MaterializeQuery query : streamQueries) {
      Preconditions.checkArgument(query.getSink() instanceof OptimizedDAG.TableSink, "Export not yet implemented");
      OptimizedDAG.TableSink tblsink = ((OptimizedDAG.TableSink) query.getSink());
      String dbSinkName = tblsink.getNameId() + "_sink";
      Preconditions.checkArgument(!ArrayUtils.contains(tEnv.listTables(),dbSinkName),"Table already defined: %s",dbSinkName);
      Table tbl = tableRegisterer.makeTable(query.getRelNode());

      Schema tblSchema = FlinkPipelineUtils.addPrimaryKey(tbl.getSchema().toSchema(), tblsink);

      TableDescriptor dbDescriptor;
      if (jdbcTempDatabase.isPresent()) {
        dbDescriptor = TableDescriptor.forConnector("jdbc")
            .schema(tblSchema)
            .option("url", jdbcTempDatabase.get().getPostgreSQLContainer().getJdbcUrl())
            .option("table-name", tblsink.getNameId())
            .option("username", jdbcTempDatabase.get().getPostgreSQLContainer().getUsername())
            .option("password", jdbcTempDatabase.get().getPostgreSQLContainer().getPassword())
            .build();
      } else {
        dbDescriptor = TableDescriptor.forConnector("jdbc")
            .schema(tblSchema)
            .option("url", jdbcConfiguration.getDbURL())
            .option("table-name", tblsink.getNameId())
            .option("username", jdbcConfiguration.getUser())
            .option("password", jdbcConfiguration.getPassword())
            .build();
      }
      tEnv.createTemporaryTable(dbSinkName, dbDescriptor);
      stmtSet.addInsert(dbSinkName, tbl);

//      DataStream<Row> changeStream = tEnv.toChangelogStream(tbl);
//      Table subTbl = tEnv.fromChangelogStream(changeStream,tblSchema, ChangelogMode.insertOnly());
//
//      TableDescriptor subDescriptor = TableDescriptor.forConnector("print")
//              .option("print-identifier",subSinkName)
//              .schema(tblSchema).build();
//      tEnv.createTemporaryTable(subSinkName, subDescriptor);
//      stmtSet.addInsert(subSinkName, subTbl);
    }

    return new FlinkStreamPhysicalPlan(stmtSet);
  }
}
