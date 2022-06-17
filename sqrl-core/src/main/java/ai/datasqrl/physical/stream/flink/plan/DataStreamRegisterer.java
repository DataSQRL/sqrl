package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine.Builder;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import java.util.List;

import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@AllArgsConstructor
public class DataStreamRegisterer extends RelShuttleImpl {

  StreamTableEnvironment tEnv;
  ImportManager importManager;
  Builder streamBuilder;

  final ErrorCollector errors = ErrorCollector.root();


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
    //TODO: resolve imports - this is broken and needs to be fixed
    SourceTableImport sourceTable = importManager.importTable(Name.system("ecommerce-data"),
        Name.system(tableName), null, errors);

    //TODO: rework based on FlinkIngestSchemaTest

//    DataStream<Raw> stream = new DataStreamProvider().getDataStream(sourceTable.getTable(),
//        streamBuilder);
//    FlinkInputHandler inputHandler = inputProvider.get(sourceTable.getSourceSchema());
//
//    SchemaValidationProcess validationProcess = new SchemaValidationProcess(schemaErrorTag,
//        sourceTable.getSourceSchema(),
//        SchemaAdjustmentSettings.DEFAULT,
//        sourceTable.getTable().getDataset().getDigest());
//
//    SingleOutputStreamOperator<Named> validate = stream.process(validationProcess);
//
//    SingleOutputStreamOperator<Row> rows = validate.map(inputHandler.getMapper(),
//            inputHandler.getTypeInformation());
//
//    tEnv.createTemporaryView(tableName, rows, inputHandler.getTableSchema());

    return super.visit(scan);
  }
}
