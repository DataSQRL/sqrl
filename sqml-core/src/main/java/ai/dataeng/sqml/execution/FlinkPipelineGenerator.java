package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.execution.flink.ingest.DataStreamProvider;
import ai.dataeng.sqml.execution.flink.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.planner.CalcitePlanner;
import ai.dataeng.sqml.parser.sqrl.schema.StreamTable;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.planner.nodes.ShredTableScan;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.schema.SchemaAdjustmentSettings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.NotNullEnforcer;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

public class FlinkPipelineGenerator {

  public Pair<StreamStatementSet, Map<Table, TableDescriptor>> createFlinkPipeline(
      List<LogicalFlinkSink> sinks,
      CalcitePlanner calcitePlanner) {
    FlinkTableConverter tbConverter = new FlinkTableConverter();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(
        org.apache.flink.configuration.Configuration.fromMap(Map.of(
            "taskmanager.memory.network.fraction", "0.4",
            "taskmanager.memory.network.max", "2gb"
        )));
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env);

    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.DROP);

    final OutputTag<SchemaValidationProcess.Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR"){};

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    Map<ai.dataeng.sqml.parser.Table, TableDescriptor> ddl = new HashMap<>();

    for (LogicalFlinkSink sink : sinks) {
      //Register data streams
      sink.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(TableScan scan) {
          String streamName = scan.getTable().getQualifiedName().get(0);
          if ((List.of(tEnv.listTables()).contains(streamName))) {
            return super.visit(scan);
          }
          org.apache.calcite.schema.Table table = calcitePlanner.getSchema().getTable(streamName, false).getTable();

          if (scan instanceof ShredTableScan) {
            org.apache.flink.table.api.Table tableShredding = tEnv.sqlQuery(
                "SELECT o._uuid, items._idx as _idx1, o._ingest_time, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
                    "FROM orders$3_stream o CROSS JOIN UNNEST(o.entries) AS items");

            tEnv.createTemporaryView("entries$4", tableShredding);
            return super.visit(scan);
          }

          if (table instanceof StreamTable) {
          //1. construct to stream, register it with tenv
          StreamTable streamTableScan = (StreamTable) table;
          ImportManager.SourceTableImport imp = streamTableScan.getTableImport();

          Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(imp.getSourceSchema());

          DataStream<SourceRecord.Raw> stream = new DataStreamProvider().getDataStream(imp.getTable(),env);

          SingleOutputStreamOperator<SourceRecord.Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, imp.getSourceSchema(),
              SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest()));

          SingleOutputStreamOperator<Row> rows = validate.map(tbConverter.getRowMapper(imp.getSourceSchema()),
              ordersSchema.getRight());

          tEnv.registerDataStream(streamName, rows);

          if (streamName.equalsIgnoreCase("orders$3_stream")) {
            org.apache.flink.table.api.Table tableShredding = tEnv.sqlQuery(
                "SELECT o._uuid, items._idx as _idx1, o._ingest_time, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
                    "FROM orders$3_stream o CROSS JOIN UNNEST(o.entries) AS items");

            tEnv.createTemporaryView("entries$4_stream", tableShredding);
          }

          }
          return super.visit(scan);
        }

      });


      String sql = RelToSql.convertToSql(sink.getInput(0)).replaceAll("\"", "`");
      System.out.println(sql);
      org.apache.flink.table.api.Table tbl = tEnv.sqlQuery(sql);
//      tbl.execute().print();

      String name = sink.getSqrlTable().name.getDisplay().toString();
      sink.setPhysicalName(name);
      List<String> tbls = List.of(tEnv.listTables());
//      JdbcDmlOptions options = new JdbcDmlOptionsBuilder().build();

      if (!(tbls.contains(name))) {
        Schema schema = addPrimaryKey(tbl.getSchema().toSchema(), sink.getSqrlTable());

        TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
            .schema(schema)
            .option("url", "jdbc:postgresql://localhost/henneberger")
            .option("table-name", name)
            .build();

        tEnv.createTable(name, descriptor);
        ddl.put(sink.getSqrlTable(), descriptor);
      } else {
        throw new RuntimeException(name + " T:" + tbls);
      }

      //add dml to flink sink.

      stmtSet.addInsert(name, tbl);
    }

    System.out.println(stmtSet.explain());

    return Pair.of(stmtSet, ddl);
  }

  private Schema addPrimaryKey(Schema toSchema, Table sqrlTable) {
    Schema.Builder builder = Schema.newBuilder();
    List<String> pks = new ArrayList<>();
    List<UnresolvedColumn> columns = toSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn column = columns.get(i);
      if (i == 0) { //bad remove this
        builder.column(column.getName(),
            ((UnresolvedPhysicalColumn) column).getDataType().notNull());
        pks.add(column.getName());
      } else {
        builder.column(column.getName(), ((UnresolvedPhysicalColumn) column).getDataType());
      }
    }


    return builder
//        .watermark(toSchema.getWatermarkSpecs().get(0).getColumnName(), toSchema.getWatermarkSpecs().get(0).getWatermarkExpression())
        .primaryKey(pks)
        .build();
  }
}
