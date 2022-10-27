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
import ai.datasqrl.physical.stream.flink.schema.FlinkTableSchemaGenerator;
import ai.datasqrl.physical.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import ai.datasqrl.plan.calcite.table.ImportedSourceTable;
import ai.datasqrl.plan.calcite.table.SourceTable;
import ai.datasqrl.plan.calcite.table.StateChangeType;
import ai.datasqrl.plan.calcite.table.StreamSourceTable;
import ai.datasqrl.schema.builder.UniversalTableBuilder;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import ai.datasqrl.schema.input.SchemaValidator;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.FlinkEnvProxy;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class TableRegisterer extends RelShuttleImpl {

  StreamTableEnvironmentImpl tEnv;
  ImportManager importManager;
  Builder streamBuilder;

  final Set<String> registeredTables = new HashSet<>();
  final ErrorCollector errors = ErrorCollector.root();
  final FlinkPhysicalPlanRewriter rewriter;

  public TableRegisterer(StreamTableEnvironmentImpl tEnv, ImportManager importManager, Builder streamBuilder) {
    this.tEnv = tEnv;
    this.importManager = importManager;
    this.streamBuilder = streamBuilder;
    this.rewriter = new FlinkPhysicalPlanRewriter(tEnv);
  }

  public Table makeTable(RelNode relNode) {
    registerSources(relNode);
    RelNode rewritten = FlinkPhysicalPlanRewriter.rewrite(tEnv, relNode);
    return FlinkEnvProxy.relNodeQuery(rewritten, tEnv);
  }

  private boolean addSource(SourceTable table) {
    if (registeredTables.contains(table.getNameId())) return false;
    registeredTables.add(table.getNameId());
    return true;
  }

  private void registerStreamSource(StreamSourceTable table) {
    if (!addSource(table)) return;

    UniversalTableBuilder tblBuilder = table.getStreamSchema();
    RelNode relnode = table.getBaseRelation();
    Table inputTable = makeTable(relnode);

    TypeInformation typeInformation = FlinkTypeInfoSchemaGenerator.INSTANCE.convertSchema(tblBuilder);
    DataStream<Row> changeStream = tEnv.toChangelogStream(inputTable)
            .filter(new ChangeFilter(table.getStateChangeType()))
            .process(new AugmentStream(),typeInformation);

    Schema tableSchema = FlinkTableSchemaGenerator.INSTANCE.convertSchema(tblBuilder);
    tEnv.createTemporaryView(table.getNameId(), changeStream, tableSchema);
  }

  private void registerImportSource(ImportedSourceTable table) {
    if (!addSource(table)) return;
    //TODO: if we are reading data in strict mode, we can use table API connectors directly which can be more efficient
    SourceTableImport imp = table.getSourceTableImport();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();
    //TODO: push down startup timestamp if determined in FlinkPhysicalPlanner
    StreamHolder<Raw> stream = streamPreparer.getRawInput(imp.getTable(),streamBuilder);
    SchemaValidator schemaValidator = new SchemaValidator(imp.getSchema(), SchemaAdjustmentSettings.DEFAULT, imp.getTable().getDataset().getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),"schema", SourceRecord.Named.class);
    streamBuilder.addAsTable(validate, imp.getSchema(), table.getNameId());

  }

  private void registerSources(RelNode relNode) {
    relNode.accept(this);
  }

  @Override
  public RelNode visit(TableScan scan) {
    SourceTable table = scan.getTable().unwrap(SourceTable.class);
    if (table instanceof ImportedSourceTable) {
      registerImportSource((ImportedSourceTable) table);
    } else {
      registerStreamSource((StreamSourceTable) table);
    }
    return super.visit(scan);
  }

  @Value
  private static class ChangeFilter implements FilterFunction<Row>, Serializable {

    StateChangeType stateChangeType;

    @Override
    public boolean filter(Row row) throws Exception {
      RowKind kind = row.getKind();
      switch (stateChangeType) {
        case ADD: return kind==RowKind.INSERT;
        case UPDATE: return kind==RowKind.UPDATE_AFTER;
        case DELETE: return kind==RowKind.DELETE;
        default: throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Adds uuid, ingest time, and source time to the row
   */
  private static class AugmentStream extends ProcessFunction<Row,Row> {

    @Override
    public void processElement(Row row, ProcessFunction<Row, Row>.Context context, Collector<Row> collector) throws Exception {
      int offset = 2;
      Object[] data = new Object[row.getArity()+offset];
      data[0] = SourceRecord.makeUUID().toString();
      data[1] = Instant.ofEpochMilli(context.timerService().currentProcessingTime()); //ingest time
      //data[2] = Instant.ofEpochMilli(context.timestamp()); //source time //TODO: why is the timestamp not propagated?
      for (int i = 0; i < row.getArity(); i++) {
        data[i+offset] = row.getField(i);
      }
      collector.collect(Row.ofKind(RowKind.INSERT,data));
    }
  }

}
