package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.execution.flink.ingest.DataStreamProvider;
import ai.dataeng.sqml.execution.flink.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.parser.RelToSql;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.planner.nodes.ShredTableScan;
import ai.dataeng.sqml.planner.nodes.StreamTableScan;
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
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcDmlOptions.JdbcDmlOptionsBuilder;
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
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

public class FlinkPipelineGenerator {

  public Pair<StreamStatementSet, Map<Table, TableDescriptor>> createFlinkPipeline(
      List<LogicalFlinkSink> sinks) {
    FlinkTableConverter tbConverter = new FlinkTableConverter();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironmentImpl tEnv = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(env);
    StreamPlanner streamPlanner = (StreamPlanner) tEnv.getPlanner();
    FlinkRelBuilder builder = streamPlanner.getRelBuilder();
    builder.getCluster();
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

          if (scan instanceof StreamTableScan) {
          //1. construct to stream, register it with tenv
          ImportManager.SourceTableImport imp = ((StreamTableScan)scan).getTableImport();
          Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(imp.getSourceSchema());

          DataStream<SourceRecord.Raw> stream = new DataStreamProvider().getDataStream(imp.getTable(),env);

          SingleOutputStreamOperator<SourceRecord.Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, imp.getSourceSchema(),
              SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest()));

          SingleOutputStreamOperator<Row> rows = validate.map(tbConverter.getRowMapper(imp.getSourceSchema()),
              ordersSchema.getRight());
//            String streamName = scan.getTable().getQualifiedName().get(0).substring(0, 4);

            tEnv.registerDataStream(streamName + "_stream", rows);
//              PlannerQueryOperation op = (PlannerQueryOperation)tEnv.getParser().parse(" + orders_stream).get(0);

            tEnv.sqlUpdate("CREATE TEMPORARY VIEW " + streamName + " AS"
                + " SELECT * FROM " + streamName + "_stream");
          } else if (scan instanceof ShredTableScan) {
            ShredTableScan ts = (ShredTableScan) scan;

            org.apache.flink.table.api.Table tableShredding = tEnv.sqlQuery(
                "SELECT o._uuid, items._idx, o.customerid, items.discount, items.quantity, items.productid, items.unit_price \n" +
                "FROM orders$1 o CROSS JOIN UNNEST(o.entries) AS items");
            tEnv.createTemporaryView("entries$2", tableShredding);
          }
          return super.visit(scan);
        }

      });

      String sql = RelToSql.convertToSql(sink.getInput(0)).replaceAll("\"", "`");
      System.out.println(sql);
      org.apache.flink.table.api.Table tbl = tEnv.sqlQuery(sql);

      String name = sink.getQueryTable().name.getDisplay().toString();
      sink.setPhysicalName(name);
      List<String> tbls = List.of(tEnv.listTables());
//      JdbcDmlOptions options = new JdbcDmlOptionsBuilder().build();

      if (!(tbls.contains(name))) {
        Schema schema = addPrimaryKey(tbl.getSchema().toSchema());

        TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
            .schema(schema)
            .option("url", "jdbc:postgresql://localhost/henneberger")
            .option("table-name", name)
            .build();

        tEnv.createTable(name, descriptor);
        ddl.put(sink.getQueryTable(), descriptor);
      } else {
        throw new RuntimeException(name + " T:" + tbls);
      }

      //add dml to flink sink.

      stmtSet.addInsert(name, tbl);
    }

    System.out.println(stmtSet.explain());

    return Pair.of(stmtSet, ddl);
  }

  private Schema addPrimaryKey(Schema toSchema) {
    Schema.Builder builder = Schema.newBuilder();
    for (UnresolvedColumn column : toSchema.getColumns()) {
      if (column.getName().equalsIgnoreCase("CUSTOMERID")) {
        builder.column(column.getName(),  ((UnresolvedPhysicalColumn) column).getDataType().notNull());
      } else {
        builder.column(column.getName(),  ((UnresolvedPhysicalColumn) column).getDataType());
      }
    }

    return builder
//        .watermark(toSchema.getWatermarkSpecs().get(0).getColumnName(), toSchema.getWatermarkSpecs().get(0).getWatermarkExpression())
        .primaryKey("CUSTOMERID")
        .build();
  }
}
