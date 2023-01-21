/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.inmemory;

import static com.datasqrl.engine.EngineCapability.CUSTOM_FUNCTIONS;
import static com.datasqrl.engine.EngineCapability.DATA_MONITORING;
import static com.datasqrl.engine.EngineCapability.DENORMALIZE;
import static com.datasqrl.engine.EngineCapability.EXTENDED_FUNCTIONS;
import static com.datasqrl.engine.EngineCapability.TEMPORAL_JOIN;
import static com.datasqrl.engine.EngineCapability.TIME_WINDOW_AGGREGATION;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.stream.FunctionWithError;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.inmemory.InMemStreamEngine.JobBuilder.Sink;
import com.datasqrl.engine.stream.inmemory.io.FileStreamUtil;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.engine.stream.monitor.MetricStore;
import com.datasqrl.engine.stream.monitor.MetricStore.Provider;
import com.datasqrl.error.ErrorCollection;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.plan.global.OptimizedDAG;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

@Slf4j
public class InMemStreamEngine extends ExecutionEngine.Base implements StreamEngine {

  public InMemStreamEngine() {
    super(InMemoryStreamConfiguration.ENGINE_NAME, ExecutionEngine.Type.STREAM,
        EnumSet.of(DENORMALIZE, TEMPORAL_JOIN,
            TIME_WINDOW_AGGREGATION, EXTENDED_FUNCTIONS, CUSTOM_FUNCTIONS, DATA_MONITORING));
  }

  @Override
  public JobBuilder createDataMonitor() {
    return new JobBuilder();
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs,
      RelBuilder relBuilder, TableSink errorSink) {
    throw new UnsupportedOperationException();
  }

  @Getter
  public class JobBuilder implements DataMonitor {

    private final List<Sink> sinks = new ArrayList<>();
    private final ErrorCollection errorHolder = new ErrorCollection();

    @Override
    public StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(TableInput table) {
      Preconditions.checkArgument(table.getParser() instanceof TextLineFormat.Parser,
          "This method only supports text sources");
      DataSystemConnector source = table.getConnector();

      if (source instanceof DirectoryConnector) {
        DirectoryConnector filesource = (DirectoryConnector) source;
        try {
          Stream<Path> paths = FileStreamUtil.matchingFiles(
              filesource.getPathConfig(),
              filesource, table.getConfiguration());
          return new Holder<>(
              FileStreamUtil.filesByline(paths).map(s -> new TimeAnnotatedRecord<>(s)));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new UnsupportedOperationException();
      }
    }

    @Override
    public <M extends Metric<M>> void monitorTable(TableInput tableSource, StreamHolder<M> stream,
        Provider<M> storeProvider) {
      Preconditions.checkArgument(stream instanceof Holder);
      Holder<M> metrics = (Holder<M>) stream;
      metrics.close();
      sinks.add(new Sink(metrics.stream, storeProvider));
    }

    @Value
    public class Sink<M extends Metric<M>> {
      Stream<M> stream;
      Provider<M> storeProvider;
    }

    @Override
    public Job build() {
      return new InMemStreamEngine.Job(sinks, errorHolder);
    }

    public class Holder<T> implements StreamHolder<T> {

      private boolean isClosed = false;
      @Getter
      private final Stream<T> stream;

      private Holder(Stream<T> stream) {
        this.stream = stream;
      }

      private void checkClosed() {
        Preconditions.checkArgument(!isClosed, "Only support single pipeline stream");
      }

      public void close() {
        isClosed = true;
      }

      private <R> Holder<R> wrap(Stream<R> newStream) {
        close();
        return new Holder<>(newStream);
      }

      @Override
      public <R> Holder<R> mapWithError(FunctionWithError<T, R> function,
          ErrorLocation errorLocation, Class<R> clazz) {
        checkClosed();
        return wrap(stream.flatMap(t -> {
          ErrorCollector collector = new ErrorCollector(errorLocation);
          Optional<R> result = Optional.empty();
          try {
            result = function.apply(t, () -> collector);
          } catch (Exception e) {
            collector.handle(e);
          }
          if (collector.hasErrors()) {
            errorHolder.addAll(collector.getErrors(), null);
          }
          if (result.isPresent()) {
            return Stream.of(result.get());
          } else {
            return Stream.empty();
          }
        }));
      }
    }
  }

  public class Job implements DataMonitor.Job {

    private Status status;

    private final List<Sink> sinks;
    @Getter
    private final ErrorCollection errorHolder;

    private Job(List<Sink> sinks, ErrorCollection errorHolder) {
      this.sinks = sinks;
      this.errorHolder = errorHolder;
      status = Status.PREPARING;
    }

    @Override
    public void execute(String name) {
      Preconditions.checkArgument(status == Status.PREPARING, "Job has already been executed");
      try {
        //Reduce all streams and write to store
        for (Sink<?> sink : sinks) {
          sink.stream.reduce((stat1, stat2) -> {
            stat1.merge(stat2);
            return stat1;
          }).ifPresent(stat -> {
            try (MetricStore store = sink.storeProvider.open()) {
              store.put(stat);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
        }
        status = Status.COMPLETED;
      } catch (Throwable e) {
        System.err.println(e);
        status = Status.FAILED;
      }
      if (errorHolder.hasErrors()) {
        errorHolder.stream().map(ErrorPrinter::prettyPrint).forEach(System.out::println);
      }
    }

    @Override
    public void cancel() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Status getStatus() {
      return status;
    }
  }

}
