/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.config.SourceServiceLoader;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.schema.FlinkRowConstructor;
import com.datasqrl.engine.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import com.datasqrl.engine.stream.flink.schema.UniversalTable2FlinkSchema;
import com.datasqrl.engine.stream.inmemory.io.FileStreamUtil;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.InputTableSchema;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

@Getter
//TODO: Do output tags (errors, monitor) need a globally unique name or just local to the job?
public class FlinkStreamBuilder implements FlinkStreamEngine.Builder {

  public static final int DEFAULT_PARALLELISM = 16;
  public static final String STATS_NAME_PREFIX = "Stats";

  private final FlinkStreamEngine engine;
  private final StreamExecutionEnvironment environment;
  private final StreamTableEnvironment tableEnvironment;
  private final UUID uuid;
  private final int defaultParallelism = DEFAULT_PARALLELISM;
  private FlinkStreamEngine.JobType jobType;

  public static final String ERROR_TAG_PREFIX = "error";
  private Map<String, OutputTag<ProcessError>> errorTags = new HashMap<>();


  public FlinkStreamBuilder(FlinkStreamEngine engine, StreamExecutionEnvironment environment) {
    this.engine = engine;
    this.environment = environment;
    this.tableEnvironment = StreamTableEnvironment.create(environment);

    this.uuid = UUID.randomUUID();
  }

  @Override
  public void setJobType(@NonNull FlinkStreamEngine.JobType jobType) {
    this.jobType = jobType;
  }

  @Override
  public FlinkStreamEngine.FlinkJob build() {
    return engine.createStreamJob(environment, jobType);
  }

  @Override
  public OutputTag<ProcessError> getErrorTag(final String errorName) {
    OutputTag<ProcessError> errorTag = errorTags.get(errorName);
    if (errorTag == null) {
      errorTag = new OutputTag<>(
          FlinkStreamEngine.getFlinkName(ERROR_TAG_PREFIX, errorName)) {
      };
      errorTags.put(errorName, errorTag);
    }
    return errorTag;
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
