/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SourceServiceLoader;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.monitor.SaveMetricsSink;
import com.datasqrl.engine.stream.flink.plan.FlinkTableRegistration.FlinkTableRegistrationContext;
import com.datasqrl.engine.stream.flink.schema.FlinkRowConstructor;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import com.datasqrl.engine.stream.flink.util.FlinkUtilities;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.engine.stream.monitor.MetricStore.Provider;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.InputTableSchema;
import com.google.common.base.Preconditions;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

@Getter
@Slf4j
public class FlinkStreamBuilder implements DataMonitor {

  public static final int DEFAULT_PARALLELISM = 16;
  public static final String STATS_NAME_PREFIX = "Stats";

  private final AbstractFlinkStreamEngine engine;
  private final StreamExecutionEnvironment environment;
  private final StreamTableEnvironmentImpl tableEnvironment;
  private final StreamStatementSet streamStatementSet;
  private final FlinkErrorHandler errorHandler;
  private final UUID uuid;
  private final int defaultParallelism = DEFAULT_PARALLELISM;


  public FlinkStreamBuilder(AbstractFlinkStreamEngine engine, StreamExecutionEnvironment environment) {
    this.engine = engine;
    this.environment = environment;
    this.tableEnvironment = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(environment);
    this.streamStatementSet = tableEnvironment.createStatementSet();
    this.errorHandler = new FlinkErrorHandler();
    this.uuid = UUID.randomUUID();
  }

  public FlinkTableRegistrationContext getContext() {
    return new FlinkTableRegistrationContext(tableEnvironment, this, streamStatementSet);
  }

  public FlinkErrorHandler getErrorHandler() {
    return errorHandler;
  }

  @Override
  public FlinkJob build() {
    //Process errors
    errorHandler.getErrorStream().ifPresent(errStream -> errStream.print());
    return new FlinkJob(environment);
  }

  public void addAsTable(StreamHolder<SourceRecord.Named> stream, InputTableSchema schema,
      String qualifiedTableName) {
    Preconditions.checkArgument(
        stream instanceof FlinkStreamHolder && ((FlinkStreamHolder) stream).getBuilder()
            .equals(this));
    FlinkStreamHolder<SourceRecord.Named> flinkStream = (FlinkStreamHolder) stream;

    //TODO: error handling when mapping doesn't work?
    UniversalTable universalTable = schema.getSchema().createUniversalTable(schema.isHasSourceTimestamp());
    Schema flinkSchema = new UniversalTable2FlinkSchema().convertSchema(universalTable);
    TypeInformation typeInformation = new FlinkTypeInfoSchemaGenerator()
        .convertSchema(universalTable);
    RowMapper rowMapper = schema.getSchema().getRowMapper(FlinkRowConstructor.INSTANCE, schema.isHasSourceTimestamp());
    DataStream rows = flinkStream.getStream()
          .map(rowMapper::apply, typeInformation);

    tableEnvironment.createTemporaryView(qualifiedTableName, rows, flinkSchema);
  }

  @Override
  public StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(TableInput table) {
    Preconditions.checkArgument(table.getParser() instanceof TextLineFormat.Parser,
        "This method only supports text sources");
    DataSystemConnector sourceConnector = table.getConnector();
    String flinkSourceName = table.getDigest().toString('-', "input");

    StreamExecutionEnvironment env = getEnvironment();
    DataStream<TimeAnnotatedRecord<String>> timedSource =
        (DataStream<TimeAnnotatedRecord<String>>)new SourceServiceLoader().load("flink", sourceConnector.getPrefix())
        .orElseThrow(()->new UnsupportedOperationException("Unrecognized source table type: " + table))
        .create(sourceConnector, new FlinkSourceFactoryContext(env, flinkSourceName, table, getUuid()));
    return new FlinkStreamHolder<>(this, timedSource);
  }

  @Override
  public <M extends Metric<M>> void monitorTable(TableInput tableSource, StreamHolder<M> stream,
      Provider<M> storeProvider) {
    Preconditions.checkArgument(stream instanceof FlinkStreamHolder);

    DataStream<M> metricsStream = ((FlinkStreamHolder) stream).getStream();

    //Process the gathered statistics in the side output
    final int randomKey = FlinkUtilities.generateBalancedKey(getDefaultParallelism());
    metricsStream
        .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
        .reduce(
            new ReduceFunction<M>() {
              @Override
              public M reduce(M base,
                  M add) throws Exception {
                base.merge(add);
                return base;
              }
            })
        .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
        //TODO: add time window to buffer before writing to database for efficiency
        .addSink(new SaveMetricsSink<M>(storeProvider));
  }


}
