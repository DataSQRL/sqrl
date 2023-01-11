/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.inmemory;

import static com.datasqrl.engine.EngineCapability.CUSTOM_FUNCTIONS;
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
import com.datasqrl.engine.stream.inmemory.io.FileStreamUtil;
import com.datasqrl.error.ErrorCollection;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Named;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystem.DirectoryConnector;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.schema.converters.RowMapper;
import com.datasqrl.schema.input.InputTableSchema;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

@Slf4j
public class InMemStreamEngine extends ExecutionEngine.Base implements StreamEngine {

  private final AtomicInteger jobIdCounter = new AtomicInteger(0);
  private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();

  public InMemStreamEngine() {
    super(InMemoryStreamConfiguration.ENGINE_NAME, ExecutionEngine.Type.STREAM,
        EnumSet.of(DENORMALIZE, TEMPORAL_JOIN,
            TIME_WINDOW_AGGREGATION, EXTENDED_FUNCTIONS, CUSTOM_FUNCTIONS));
  }

  @Override
  public JobBuilder createJob() {
    return new JobBuilder();
  }

  @Override
  public Optional<? extends Job> getJob(String id) {
    return Optional.ofNullable(jobs.get(id));
  }

  @Override
  public void close() throws IOException {
    jobs.clear();
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
  public class JobBuilder implements StreamEngine.Builder {

    private final List<Stream> mainStreams = new ArrayList<>();
    private final List<Stream> sideStreams = new ArrayList<>();
    private final ErrorCollection errorHolder = new ErrorCollection();
    private final RecordHolder recordHolder = new RecordHolder();

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
    public void addAsTable(StreamHolder<SourceRecord.Named> stream, InputTableSchema schema,
        String qualifiedTableName) {
      final Consumer<Object[]> records = recordHolder.getCollector(qualifiedTableName);
      RowMapper<Object[]> mapper = schema.getSchema().getRowMapper(InMemRowConstructor.INSTANCE, schema.isHasSourceTimestamp());
      ((Holder<Named>) stream).mapWithError((r, c) -> {
        try {
          records.accept(mapper.apply(r));
          return Optional.of(Boolean.TRUE);
        } catch (Exception e) {
          ErrorCollector errors = c.get();
          errors.fatal(e.toString());
          return Optional.of(Boolean.FALSE);
        }
      }, ErrorPrefix.INPUT_DATA.resolve(qualifiedTableName), Boolean.class).sink();
    }

    @Override
    public Job build() {
      return new Job(mainStreams, sideStreams, errorHolder, recordHolder);
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

      @Override
      public void printSink() {
        checkClosed();
        wrap(stream.map(r -> {
          log.trace("{}", r);
          return r;
        })).sink();
      }

      private void sink() {
        checkClosed();
        close();
        mainStreams.add(stream);
      }
    }
  }

  public static class RecordHolder extends OutputCollector<String, Object[]> {

  }

  public class Job implements StreamEngine.Job {

    private final String id;
    private Status status;

    private final List<Stream> mainStreams;
    private final List<Stream> sideStreams;
    @Getter
    private final ErrorCollection errorHolder;
    @Getter
    private final RecordHolder recordHolder;

    private Job(List<Stream> mainStreams, List<Stream> sideStreams, ErrorCollection errorHolder,
        RecordHolder recordHolder) {
      this.mainStreams = mainStreams;
      this.sideStreams = sideStreams;
      this.errorHolder = errorHolder;
      this.recordHolder = recordHolder;
      id = Integer.toString(jobIdCounter.incrementAndGet());
      jobs.put(id, this);
      status = Status.PREPARING;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public void execute(String name) {
      Preconditions.checkArgument(status == Status.PREPARING, "Job has already been executed");
      try {
        //Execute main streams first and side streams after
        for (Stream stream : mainStreams) {
          stream.forEach(s -> {
          });
        }
        for (Stream stream : sideStreams) {
          stream.forEach(s -> {
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

  public static class InMemRowConstructor implements
      com.datasqrl.schema.converters.RowConstructor<Object[]> {

    private static final InMemRowConstructor INSTANCE = new InMemRowConstructor();

    @Override
    public Object[] createRow(Object[] columns) {
      return columns;
    }

    @Override
    public List createNestedRow(Object[] columns) {
      return Arrays.asList(columns);
    }

    @Override
    public List createRowList(Object[] rows) {
      return Arrays.asList(rows);
    }
  }

}
