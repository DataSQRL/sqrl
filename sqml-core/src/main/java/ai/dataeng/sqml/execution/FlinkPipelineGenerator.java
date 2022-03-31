package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.execution.flink.ingest.DataStreamProvider;
import ai.dataeng.sqml.execution.flink.ingest.SchemaValidationProcess;
import ai.dataeng.sqml.execution.flink.ingest.schema.FlinkTableConverter;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.parser.RelToSql;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.planner.nodes.StreamTableScan;
import ai.dataeng.sqml.tree.name.ReservedName;
import ai.dataeng.sqml.tree.name.VersionedName;
import ai.dataeng.sqml.type.schema.SchemaAdjustmentSettings;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.types.AtomicDataType;
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
          if (scan instanceof StreamTableScan) {
            //1. construct to stream, register it with tenv
            ImportManager.SourceTableImport imp = ((StreamTableScan)scan).getTableImport();
            Pair<Schema, TypeInformation> ordersSchema = tbConverter.tableSchemaConversion(imp.getSourceSchema());

            DataStream<SourceRecord.Raw> stream = new DataStreamProvider().getDataStream(imp.getTable(),env);
            SingleOutputStreamOperator<SourceRecord.Named> validate = stream.process(new SchemaValidationProcess(schemaErrorTag, imp.getSourceSchema(),
                SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest()));

            SingleOutputStreamOperator<Row> rows = validate.map(tbConverter.getRowMapper(imp.getSourceSchema()),
                ordersSchema.getRight());

            List<String> tbls = List.of(tEnv.listTables());
            if (!(tbls.contains("orders"))) {
              String fields = ordersSchema.getKey().getColumns().stream()
                  .map(c->(UnresolvedPhysicalColumn) c)
                  .filter(c->c.getDataType() instanceof AtomicDataType)
                  .filter(c->!c.getName().equals(ReservedName.INGEST_TIME.getCanonical()) &&!c.getName().equals(ReservedName.SOURCE_TIME.getCanonical()))
                  .map(c->String.format("`"+c.getName()+"` AS `%s`", c.getName() + VersionedName.ID_DELIMITER + "0"))
                  .collect(Collectors.joining(", "));

              tEnv.registerDataStream("orders_stream", rows);
//              PlannerQueryOperation op = (PlannerQueryOperation)tEnv.getParser().parse(" + orders_stream).get(0);

              tEnv.sqlUpdate("CREATE TEMPORARY VIEW orders AS SELECT "+fields+" FROM orders_stream");
            }
          }
          return super.visit(scan);
        }

      });

      org.apache.flink.table.api.Table tbl = tEnv.sqlQuery(
          RelToSql.convertToSql(sink.getInput(0)).replaceAll("\"", "`"));

      String name = sink.getQueryTable().getName().getCanonical() + "_Sink";
      sink.setPhysicalName(name);
      List<String> tbls = List.of(tEnv.listTables());
      if (!(tbls.contains(name))) {
        TableDescriptor descriptor = TableDescriptor.forConnector("jdbc")
            .schema(tbl.getSchema().toSchema())
            .option("url", "jdbc:postgresql://localhost/henneberger")
            .option("table-name", name)
            .build();
        tEnv.createTable(name, descriptor);
        ddl.put(sink.getQueryTable(), descriptor);
      }

      //add dml to flink sink.

      stmtSet.addInsert(name, tbl);
    }


    return Pair.of(stmtSet, ddl);
  }
}
