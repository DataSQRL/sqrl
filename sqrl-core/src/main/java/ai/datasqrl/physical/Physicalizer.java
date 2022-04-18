package ai.datasqrl.physical;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine.Builder;
import ai.datasqrl.execute.flink.environment.LocalFlinkStreamEngineImpl;
import ai.datasqrl.execute.flink.ingest.DataStreamProvider;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess.Error;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.io.sources.SourceRecord.Named;
import ai.datasqrl.io.sources.SourceRecord.Raw;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.FlinkPipelineGenerator;
import ai.datasqrl.plan.LogicalPlan;
import ai.datasqrl.plan.RelQuery;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.type.schema.SchemaAdjustmentSettings;
import ai.datasqrl.sql.RelToSql;
import ai.datasqrl.validate.imports.ImportManager;
import ai.datasqrl.validate.imports.ImportManager.SourceTableImport;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

@AllArgsConstructor
public class Physicalizer {
  ImportManager importManager;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;

  public ExecutionPlan plan(LogicalPlan plan) {
    List<SqlDDLStatement> databaseDDL = createDatabaseDDL(plan.getDatabaseQueries());
    StreamStatementSet streamQueries = createStreamJobGraph(plan.getStreamQueries());

    return new ExecutionPlan(databaseDDL, streamQueries, plan.getSchema());
  }


  private List<SqlDDLStatement> createDatabaseDDL(List<RelQuery> databaseQueries) {
    return null;
  }

  private StreamStatementSet createStreamJobGraph(List<RelQuery> streamQueries) {
    Builder streamBuilder = ((LocalFlinkStreamEngineImpl)streamEngine).createStream();
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)
        StreamTableEnvironment.create(streamBuilder.getEnvironment());

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.ERROR);

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    for (RelQuery sink : streamQueries) {
      registerStream(sink, tEnv, streamBuilder);

      String sql = RelToSql.convertToSql(sink.getRelNode().getInput(0)).replaceAll("\"", "`");
      System.out.println(sql);
      org.apache.flink.table.api.Table tbl = tEnv.sqlQuery(sql);

      String name = sink.getTable().name.getCanonical()+"_sink";

      Schema schema = FlinkPipelineGenerator.addPrimaryKey(tbl.getSchema().toSchema(), sink.getTable());

      TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
          .schema(schema)
          .option("url", "jdbc:postgresql://localhost/henneberger")
          .option("table-name", sink.getTable().name.getCanonical())
          .build();

      //Create sink
      tEnv.createTable(name, descriptor);

      stmtSet.addInsert(name, tbl);
    }

    return stmtSet;
  }

  public void registerStream(RelQuery sink,
      StreamTableEnvironmentImpl tEnv,
      Builder streamBuilder) {
    ErrorCollector errors = ErrorCollector.root();

    final OutputTag<Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR") {
    };
    FlinkTableConverter tbConverter = new FlinkTableConverter();

    sink.getRelNode().accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        String streamName = scan.getTable().getQualifiedName().get(0);
        if ((List.of(tEnv.listTables()).contains(streamName))) {
          return super.visit(scan);
        }
        NamePath path = NamePath.parse(streamName);
        //TODO: resolve imports
        SourceTableImport sourceTable = importManager.resolveTable2(Name.system("ecommerce-data"), path.get(0),
            Optional.empty(), errors);

        DataStream<Raw> stream = new DataStreamProvider().getDataStream(sourceTable.getTable(),
            streamBuilder);
        Pair<Schema, TypeInformation> tableSchema = tbConverter.tableSchemaConversion(
            sourceTable.getSourceSchema());
        SingleOutputStreamOperator<Named> validate = stream.process(
            new SchemaValidationProcess(schemaErrorTag, sourceTable.getSourceSchema(),
                SchemaAdjustmentSettings.DEFAULT,
                sourceTable.getTable().getDataset().getDigest()));

        SingleOutputStreamOperator<Row> rows = validate.map(
            tbConverter.getRowMapper(sourceTable.getSourceSchema()),
            tableSchema.getRight());

        tEnv.registerDataStream(streamName, rows);

        return super.visit(scan);
      }
    });
  }
}
