package ai.datasqrl.physical.stream.flink.plan;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.SourceRecord.Raw;
import ai.datasqrl.io.sources.util.StreamInputPreparer;
import ai.datasqrl.io.sources.util.StreamInputPreparerImpl;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.physical.stream.flink.FlinkStreamEngine.Builder;
import ai.datasqrl.plan.calcite.table.ImportedSourceTable;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.schema.input.SchemaValidator;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

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

    //TODO: rework based on FlinkIngestSchemaTest

    ImportedSourceTable t = scan.getTable().unwrap(ImportedSourceTable.class);
    SourceTableImport imp = t.getSourceTableImport();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();

    StreamHolder<Raw> stream = streamPreparer.getRawInput(imp.getTable(),streamBuilder);
    SchemaValidator schemaValidator = new SchemaValidator(imp.getSchema(), SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),"schema", SourceRecord.Named.class);
    streamBuilder.addAsTable(validate, imp.getSchema(), tableName);

    return super.visit(scan);
  }
}
