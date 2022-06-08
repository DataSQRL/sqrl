package ai.datasqrl.physical.stream;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine.Builder;
import ai.datasqrl.execute.flink.ingest.DataStreamProvider;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess;
import ai.datasqrl.execute.flink.ingest.SchemaValidationProcess.Error;
import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.io.sources.SourceRecord.Named;
import ai.datasqrl.io.sources.SourceRecord.Raw;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

@AllArgsConstructor
public class DataStreamRegisterer extends RelShuttleImpl {

  StreamTableEnvironment tEnv;
  ImportManager importManager;
  Builder streamBuilder;

  final ErrorCollector errors = ErrorCollector.root();

  final OutputTag<Error> schemaErrorTag = new OutputTag<>("SCHEMA_ERROR") {
  };
  final FlinkTableConverter tbConverter = new FlinkTableConverter();

  public void register(RelNode relNode) {
    relNode.accept(this);
  }

  @Override
  public RelNode visit(TableScan scan) {

    String streamName = String.join(".", scan.getTable().getQualifiedName());
    String tableName = scan.getTable().getQualifiedName()
        .get(scan.getTable().getQualifiedName().size() - 1);
    if ((List.of(tEnv.listTables()).contains(tableName))) {
      return super.visit(scan);
    }
//        NamePath path = NamePath.parse(streamName);
    //TODO: resolve imports
    SourceTableImport sourceTable = importManager.resolveTable(Name.system("ecommerce-data"),
        Name.system(tableName),
        Optional.empty(), errors);

    DataStream<Raw> stream = new DataStreamProvider().getDataStream(sourceTable.getTable(),
        streamBuilder);
    Pair<Schema, TypeInformation> tableSchema = tbConverter.tableSchemaConversion(
        sourceTable.getSourceSchema());

    SchemaValidationProcess validationProcess = new SchemaValidationProcess(schemaErrorTag,
        sourceTable.getSourceSchema(),
        SchemaAdjustmentSettings.DEFAULT,
        sourceTable.getTable().getDataset().getDigest());

    SingleOutputStreamOperator<Named> validate = stream.process(validationProcess);

    SingleOutputStreamOperator<Row> rows = validate.map(
        tbConverter.getRowMapper(sourceTable.getSourceSchema()),
        tableSchema.getRight());

    tEnv.createTemporaryView(tableName, rows, tableSchema.getKey());

    return super.visit(scan);
  }
}
