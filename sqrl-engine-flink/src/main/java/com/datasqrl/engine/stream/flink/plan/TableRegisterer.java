/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine.Builder;
import com.datasqrl.engine.stream.flink.schema.FlinkTableSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.StreamInputPreparer;
import com.datasqrl.io.util.StreamInputPreparerImpl;
import com.datasqrl.plan.calcite.table.ImportedRelationalTable;
import com.datasqrl.plan.calcite.table.SourceRelationalTable;
import com.datasqrl.plan.calcite.table.StateChangeType;
import com.datasqrl.plan.calcite.table.StreamRelationalTable;
import com.datasqrl.schema.UniversalTableBuilder;
import com.datasqrl.schema.input.SchemaValidator;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
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

public class TableRegisterer extends RelShuttleImpl {

  StreamTableEnvironmentImpl tEnv;
  Builder streamBuilder;

  final Set<String> registeredTables = new HashSet<>();
  final ErrorCollector errors = ErrorCollector.root();
  final FlinkPhysicalPlanRewriter rewriter;

  public TableRegisterer(StreamTableEnvironmentImpl tEnv, Builder streamBuilder) {
    this.tEnv = tEnv;
    this.streamBuilder = streamBuilder;
    this.rewriter = new FlinkPhysicalPlanRewriter(tEnv);
  }

  public Table makeTable(RelNode relNode) {
    registerSources(relNode);
    RelNode rewritten = FlinkPhysicalPlanRewriter.rewrite(tEnv, relNode);
    return FlinkEnvProxy.relNodeQuery(rewritten, tEnv);
  }

  private boolean addSource(SourceRelationalTable table) {
    if (registeredTables.contains(table.getNameId())) {
      return false;
    }
    registeredTables.add(table.getNameId());
    return true;
  }

  private void registerStreamSource(StreamRelationalTable table) {
    if (!addSource(table)) {
      return;
    }

    UniversalTableBuilder tblBuilder = table.getStreamSchema();
    RelNode relnode = table.getBaseRelation();
    Table inputTable = makeTable(relnode);

    TypeInformation typeInformation = FlinkTypeInfoSchemaGenerator.INSTANCE.convertSchema(
        tblBuilder);
    DataStream<Row> changeStream = tEnv.toChangelogStream(inputTable)
        .filter(new ChangeFilter(table.getStateChangeType()))
        .process(new AugmentStream(), typeInformation);

    Schema tableSchema = FlinkTableSchemaGenerator.INSTANCE.convertSchema(tblBuilder);
    tEnv.createTemporaryView(table.getNameId(), changeStream, tableSchema);
  }

  private void registerImportSource(ImportedRelationalTable table) {
    if (!addSource(table)) {
      return;
    }
    //TODO: if we are reading data in strict mode, we can use table API connectors directly which can be more efficient
    TableSource tableSource = table.getTableSource();
    StreamInputPreparer streamPreparer = new StreamInputPreparerImpl();
    //TODO: push down startup timestamp if determined in FlinkPhysicalPlanner
    ErrorLocation errorLocation = ErrorPrefix.INPUT_DATA.resolve(tableSource.getName());
    StreamHolder<Raw> stream = streamPreparer.getRawInput(tableSource, streamBuilder,
        errorLocation);
    SchemaValidator schemaValidator = new SchemaValidator(tableSource.getSchema(),
        tableSource.getConfiguration().getSchemaAdjustmentSettings(),
        tableSource.getDigest());
    StreamHolder<SourceRecord.Named> validate = stream.mapWithError(schemaValidator.getFunction(),
        "schema", errorLocation, SourceRecord.Named.class);
    streamBuilder.addAsTable(validate, tableSource.getSchema(), table.getNameId());

  }

  private void registerSources(RelNode relNode) {
    relNode.accept(this);
  }

  @Override
  public RelNode visit(TableScan scan) {
    SourceRelationalTable table = scan.getTable().unwrap(SourceRelationalTable.class);
    if (table instanceof ImportedRelationalTable) {
      registerImportSource((ImportedRelationalTable) table);
    } else {
      registerStreamSource((StreamRelationalTable) table);
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
        case ADD:
          return kind == RowKind.INSERT;
        case UPDATE:
          return kind == RowKind.UPDATE_AFTER;
        case DELETE:
          return kind == RowKind.DELETE;
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Adds uuid, ingest time, and source time to the row
   */
  private static class AugmentStream extends ProcessFunction<Row, Row> {

    @Override
    public void processElement(Row row, ProcessFunction<Row, Row>.Context context,
        Collector<Row> collector) throws Exception {
      int offset = 2;
      Object[] data = new Object[row.getArity() + offset];
      data[0] = SourceRecord.makeUUID().toString();
      data[1] = Instant.ofEpochMilli(context.timerService().currentProcessingTime()); //ingest time
      //data[2] = Instant.ofEpochMilli(context.timestamp()); //source time //TODO: why is the timestamp not propagated?
      for (int i = 0; i < row.getArity(); i++) {
        data[i + offset] = row.getField(i);
      }
      collector.collect(Row.ofKind(RowKind.INSERT, data));
    }
  }

}
