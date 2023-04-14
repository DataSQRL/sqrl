/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink;

import com.datasqrl.FlinkSourceFactoryContext;
import com.datasqrl.config.SourceServiceLoader;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.flink.monitor.SaveMetricsSink;
import com.datasqrl.engine.stream.flink.util.FlinkUtilities;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.engine.stream.monitor.MetricStore.Provider;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.util.Metric;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.google.common.base.Preconditions;
import java.util.UUID;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Getter
@Slf4j
public class FlinkStreamBuilder implements DataMonitor {

  public static final int DEFAULT_PARALLELISM = 16;
  public static final String STATS_NAME_PREFIX = "Stats";

  private final AbstractFlinkStreamEngine engine;
  private final StreamExecutionEnvironment environment;
  private final StreamTableEnvironment tableEnvironment;
  private final StreamStatementSet streamStatementSet;
  private final FlinkErrorHandler errorHandler;
  private final UUID uuid;
  private final int defaultParallelism = DEFAULT_PARALLELISM;


  public FlinkStreamBuilder(AbstractFlinkStreamEngine engine, StreamExecutionEnvironment environment) {
    this.engine = engine;
    this.environment = environment;
    this.tableEnvironment = StreamTableEnvironment.create(environment);
    this.streamStatementSet = tableEnvironment.createStatementSet();
    this.errorHandler = new FlinkErrorHandler();
    this.uuid = UUID.randomUUID();
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

  @Override
  public StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(TableInput table) {
    Preconditions.checkArgument(table.getParser() instanceof TextLineFormat.Parser,
        "This method only supports text sources");
    DataSystemConnector sourceConnector = table.getConnector();
    String flinkSourceName = table.getDigest().toString('-', "input");

    StreamExecutionEnvironment env = getEnvironment();
    DataStream<TimeAnnotatedRecord<String>> timedSource =
        (DataStream<TimeAnnotatedRecord<String>>)new SourceServiceLoader().load("flink", sourceConnector.getSystemType())
        .orElseThrow(()->new UnsupportedOperationException("Unrecognized source table type: " + table))
        .create(sourceConnector, new FlinkSourceFactoryContext(env, flinkSourceName,
            table.getConfiguration().getFormat(), table.getConfiguration(), getUuid()));
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
