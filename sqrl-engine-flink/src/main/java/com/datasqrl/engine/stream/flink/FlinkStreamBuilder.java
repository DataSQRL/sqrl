/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SourceServiceLoader;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine.ErrorHandler;
import com.datasqrl.engine.stream.flink.plan.FlinkTableRegistration.FlinkTableRegistrationContext;
import com.datasqrl.engine.stream.flink.schema.FlinkRowConstructor;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.InputTableSchema;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.util.OutputTag;

@Getter
@Slf4j
public class FlinkStreamBuilder implements FlinkStreamEngine.Builder {

  public static final int DEFAULT_PARALLELISM = 16;
  public static final String STATS_NAME_PREFIX = "Stats";

  private final FlinkStreamEngine engine;
  private final StreamExecutionEnvironment environment;
  private final StreamTableEnvironmentImpl tableEnvironment;
  private final StreamStatementSet streamStatementSet;
  private final UUID uuid;
  private final int defaultParallelism = DEFAULT_PARALLELISM;
  private FlinkStreamEngine.JobType jobType;

  public static final String ERROR_TAG_PREFIX = "_errors";

  private final List<DataStream<InputError>> errorStreams = new ArrayList<>();
  @Setter
  private TableSink errorSink = null;


  public FlinkStreamBuilder(FlinkStreamEngine engine, StreamExecutionEnvironment environment) {
    this.engine = engine;
    this.environment = environment;
    this.tableEnvironment = (StreamTableEnvironmentImpl) StreamTableEnvironment.create(environment);
    this.streamStatementSet = tableEnvironment.createStatementSet();

    this.uuid = UUID.randomUUID();
  }

  @Override
  public FlinkTableRegistrationContext getContext() {
    return new FlinkTableRegistrationContext(tableEnvironment, this, streamStatementSet);
  }

  @Override
  public ErrorHandler getErrorHandler() {
    return new ErrorHandler() {

      final AtomicInteger counter = new AtomicInteger(0);

      @Override
      public OutputTag<InputError> getTag() {
        return new OutputTag<>(
            FlinkStreamEngine.getFlinkName(ERROR_TAG_PREFIX, String.valueOf(counter.incrementAndGet()))) {
        };
      }

      @Override
      public void registerErrorStream(DataStream<InputError> errorStream) {
        errorStreams.add(errorStream);
      }
    };
  }

  @Override
  public void setJobType(@NonNull FlinkStreamEngine.JobType jobType) {
    this.jobType = jobType;
  }

  @Override
  public Optional<DataStream<InputError>> getErrorStream() {
    if (errorStreams.size()>0) {
      DataStream<InputError> combinedStream = errorStreams.get(0);
      if (errorStreams.size() > 1) {
        combinedStream = combinedStream.union(
            errorStreams.subList(1, errorStreams.size()).toArray(size -> new DataStream[size]));
      }
      return Optional.of(combinedStream);
    } else return Optional.empty();
  }

  @Override
  public FlinkStreamEngine.FlinkJob build() {
    //Process errors
    getErrorStream().ifPresent(errStream -> errStream.print());
    return engine.createStreamJob(environment, jobType);
  }

  @Override
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


  @Value
  public static class SchemaToStreamContext {
    StreamTableEnvironment tEnv;
    StreamHolder<SourceRecord.Named> stream;
    InputTableSchema schema;
    RowConstructor rowConstructor;
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

  public static class NoTimedRecord implements MapFunction<String, TimeAnnotatedRecord<String>> {

    @Override
    public TimeAnnotatedRecord<String> map(String s) throws Exception {
      return new TimeAnnotatedRecord<>(s, null);
    }
  }

}
