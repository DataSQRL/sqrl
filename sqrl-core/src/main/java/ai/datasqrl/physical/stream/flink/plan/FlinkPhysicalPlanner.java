package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.io.formats.FormatConfiguration;
import ai.datasqrl.io.impl.file.DirectoryDataSystem;
import ai.datasqrl.io.impl.print.PrintDataSystem;
import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine;
import ai.datasqrl.plan.global.OptimizedDAG;
import com.google.common.base.Strings;
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

@AllArgsConstructor
public class FlinkPhysicalPlanner {

  private final StreamEngine streamEngine;
  private final JDBCConnectionProvider jdbcConfiguration;

  public FlinkStreamPhysicalPlan createStreamGraph(List<OptimizedDAG.MaterializeQuery> streamQueries) {
    final FlinkStreamEngine.Builder streamBuilder = (FlinkStreamEngine.Builder) streamEngine.createJob();
    final StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)streamBuilder.getTableEnvironment();
    final TableRegisterer tableRegisterer = new TableRegisterer(tEnv, streamBuilder);

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    //TODO: push down filters across queries to determine if we can constraint sources by time for efficiency (i.e. only load the subset of the stream that is required)
    for (OptimizedDAG.MaterializeQuery query : streamQueries) {
      String flinkSinkName = query.getSink().getName() + "_sink";
      Preconditions.checkArgument(!ArrayUtils.contains(tEnv.listTables(),flinkSinkName),"Table already defined: %s",flinkSinkName);
      Table tbl = tableRegisterer.makeTable(query.getRelNode());

      Schema tblSchema = tbl.getSchema().toSchema();

      TableDescriptor sinkDescriptor;
      if (query.getSink() instanceof  OptimizedDAG.DatabaseSink) {
        OptimizedDAG.DatabaseSink dbSink = ((OptimizedDAG.DatabaseSink) query.getSink());
        tblSchema = FlinkPipelineUtils.addPrimaryKey(tblSchema, dbSink);
        sinkDescriptor = TableDescriptor.forConnector("jdbc")
                .schema(tblSchema)
                .option("url", jdbcConfiguration.getDbURL())
                .option("table-name", dbSink.getName())
                .option("username", jdbcConfiguration.getUser())
                .option("password", jdbcConfiguration.getPassword())
                .build();
      } else {
        OptimizedDAG.ExternalSink extSink = (OptimizedDAG.ExternalSink) query.getSink();
        TableSink tableSink = extSink.getSink();
        if (tableSink.getConnector() instanceof PrintDataSystem.Connector) {
          PrintDataSystem.Connector connector = (PrintDataSystem.Connector) tableSink.getConnector();
          String name = tableSink.getName().getDisplay();
          if (!Strings.isNullOrEmpty(connector.getPrefix())) name = connector.getPrefix() + "_" + name;
          sinkDescriptor = TableDescriptor.forConnector("print")
                  .schema(tblSchema)
                  .option("print-identifier", name)
                  .build();
        } else if (tableSink.getConnector() instanceof DirectoryDataSystem.Connector) {
          DirectoryDataSystem.Connector connector = (DirectoryDataSystem.Connector) tableSink.getConnector();
          TableDescriptor.Builder tblBuilder = TableDescriptor.forConnector("filesystem")
                  .schema(tblSchema)
                  .option("path", connector.getPath().resolve(tableSink.getConfiguration().getIdentifier()).toString());
          addFormat(tblBuilder, tableSink.getConfiguration().getFormat());
          sinkDescriptor = tblBuilder.build();
        } else {
          throw new UnsupportedOperationException("Not yet implemented: " + tableSink.getConnector().getClass());
        }
      }
      tEnv.createTemporaryTable(flinkSinkName, sinkDescriptor);
      stmtSet.addInsert(flinkSinkName, tbl);
    }

    return new FlinkStreamPhysicalPlan(stmtSet);
  }

  private void addFormat(TableDescriptor.Builder tblBuilder, FormatConfiguration formatConfig) {
    switch (formatConfig.getFileFormat()) {
      case CSV:
        tblBuilder.format("csv");
        break;
      case JSON:
        tblBuilder.format("json");
        break;
      default: throw new UnsupportedOperationException("Unsupported format: " + formatConfig.getFileFormat());
    }
  }


}
