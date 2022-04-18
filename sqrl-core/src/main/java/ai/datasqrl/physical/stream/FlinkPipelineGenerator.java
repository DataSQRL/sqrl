package ai.datasqrl.physical.stream;

import ai.datasqrl.execute.flink.environment.LocalFlinkStreamEngineImpl;
import ai.datasqrl.execute.flink.ingest.DataStreamProvider;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.sql.RelToSql;
import ai.datasqrl.schema.Table;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.nodes.StreamTable;
import ai.datasqrl.plan.nodes.LogicalFlinkSink;
import ai.datasqrl.plan.nodes.ShredTableScan;
import ai.datasqrl.parse.tree.name.VersionedName;
import ai.datasqrl.schema.type.schema.SchemaAdjustmentSettings;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine.Builder;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess.Error;
import ai.datasqrl.io.sources.SourceRecord.Named;
import ai.datasqrl.io.sources.SourceRecord.Raw;
import ai.datasqrl.validate.imports.ImportManager.SourceTableImport;
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

  public static Pair<StreamStatementSet, Map<Table, TableDescriptor>> createFlinkPipeline(
      List<LogicalFlinkSink> sinks,
      CalcitePlanner calcitePlanner) {

    FlinkTableConverter tbConverter = new FlinkTableConverter();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(
        org.apache.flink.configuration.Configuration.fromMap(Map.of(
            "taskmanager.memory.network.fraction", "0.4",
            "taskmanager.memory.network.max", "2gb"
        )));
    LocalFlinkStreamEngineImpl flink = new LocalFlinkStreamEngineImpl();
    Builder streamBuilder = flink.createStream();
    StreamExecutionEnvironment senv = streamBuilder.getEnvironment();
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl)StreamTableEnvironment.create(senv);
    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER, NotNullEnforcer.DROP);

    final OutputTag<Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR"){};

    StreamStatementSet stmtSet = tEnv.createStatementSet();
    Map<Table, TableDescriptor> ddl = new HashMap<>();

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

          if (scan instanceof ShredTableScan && List.of(tEnv.listTables()).contains("orders$3_stream")) {
            org.apache.flink.table.api.Table tableShredding = tEnv.sqlQuery(
                "SELECT o._uuid, items._idx as _idx1, o._ingest_time, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
                    "FROM orders$3_stream o CROSS JOIN UNNEST(o.entries) AS items");

            tEnv.createTemporaryView("entries$4", tableShredding);
            return super.visit(scan);
          }

          if (table instanceof StreamTable) {
          //1. construct to stream, register it with tenv
          StreamTable streamTableScan = (StreamTable) table;
          SourceTableImport imp = streamTableScan.getTableImport();
          if (imp == null) {
            System.out.println(table);
            return super.visit(scan);
          }

          Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(imp.getSourceSchema());

          DataStream<Raw> stream = new DataStreamProvider().getDataStream(imp.getTable(),streamBuilder);

          SingleOutputStreamOperator<Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, imp.getSourceSchema(),
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
            if (streamName.equalsIgnoreCase("orders$3")) {
              tEnv.registerDataStream(streamName+"_stream", rows);

              org.apache.flink.table.api.Table tableShredding = tEnv.sqlQuery(
                  "SELECT o._uuid, items._idx as _idx1, o._ingest_time, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
                      "FROM orders$3_stream o CROSS JOIN UNNEST(o.entries) AS items");

              tEnv.createTemporaryView("entries$4", tableShredding);
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

//    System.out.println(stmtSet.explain());

    return Pair.of(stmtSet, ddl);
  }

  private static Schema addPrimaryKey(Schema toSchema, Table sqrlTable) {
    Schema.Builder builder = Schema.newBuilder();
    List<String> pks = new ArrayList<>();
    List<UnresolvedColumn> columns = toSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      UnresolvedColumn column = columns.get(i);
      VersionedName name = VersionedName.parse(column.getName());
      if (sqrlTable.getField(name) != null) {
        Field field = sqrlTable.getField(name);
        if (field instanceof Column && ((Column)field).isPrimaryKey()) {
          builder.column(column.getName(),
              ((UnresolvedPhysicalColumn) column).getDataType().notNull());
          pks.add(column.getName());

        } else {
          builder.column(column.getName(), ((UnresolvedPhysicalColumn) column).getDataType());
        }
      } else {
        throw new RuntimeException("?");
      }
    }


    return builder
//        .watermark(toSchema.getWatermarkSpecs().get(0).getColumnName(), toSchema.getWatermarkSpecs().get(0).getWatermarkExpression())
        .primaryKey(pks)
        .build();
  }
}
